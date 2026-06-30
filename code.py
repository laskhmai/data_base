"""
MongoDB Atlas Rightsizing Engine
=================================
Version : v8 FINAL
Source  : [Metrics].[MongoDBRightsizingAggregated5Min]
Author  : COSD Team — Humana DevOps 9009227

Flow:
  1. Load aggregated metrics per cluster per hour
  2. Calculate CPU / Memory / Connection recommendations
  3. Insert into [Metrics].[MongoDBRightsizingRecommendations]
  4. Call usp_MongoDBRightsizingSimulatedMetrics
  5. Call usp_MongoDBRightsizingEfficiency

All Changes from v7:
  - MetaConfig: keyed by Instance (not SkuName) → matches InstanceSize
  - MetaConfig: removed unused SkuName, Provider, Region columns from SELECT
  - ordered_tiers: excludes Low-CPU only (Standard + NVMe included)
  - process_cluster: filters ordered_tiers to same tier as current SKU
  - find_low_cpu_sku: removed wrong Provider/Region check (specs already filtered)
  - component_comment: fixed logic (val > current_idx = Intensive)
  - peak_val threshold: 50 → 80 (ScaleUp only on genuinely high load)
  - Low-CPU skip threshold: 80% peak CPU
  - Connection-only upsize: → NoChange + comment
  - RecommendedSku: "M50, M50-low-CPU" format
  - LowCpuSku, LowCpuSavings: new columns
  - OutsideEfficiency → LowCpuEfficiency (renamed)
  - OverallDifferentVersionSavings → LowCpuSavings (renamed)
  - calculate_efficiency: added vCores projection
  - call_stored_proc: conn.autocommit = True (fixes Synapse DDL error)
"""

import json
import warnings
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd
import pyodbc
from sklearn.cluster import KMeans

warnings.filterwarnings("ignore")

# ===========================================================================
# 1. DATABASE CONNECTION
# ===========================================================================

SERVER   = "hybridasa.sql.azuresynapse.net"
DATABASE = "hybridasa_dedicatedpool"
USERNAME = "hybridasawrite"
PASSWORD = "H@Sh1CoRS!"
DRIVER   = "{ODBC Driver 17 for SQL Server}"


# ===========================================================================
# FEATURE FLAGS
# Toggle on/off specific metric recommendations
# Set to True  = skip that metric for ALL clusters
# Set to False = use that metric normally
# ===========================================================================
SKIP_MEMORY_RECOMMENDATIONS = True   # Neeraja: do not consider memory until confirmed


def connect_to_db():
    try:
        conn = pyodbc.connect(
            f"DRIVER={DRIVER};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"UID={USERNAME};"
            f"PWD={PASSWORD};"
        )
        return conn
    except Exception as exc:
        print(f"Error connecting to database: {exc}")
        raise


def fetch_data(sql_query: str) -> pd.DataFrame:
    try:
        conn = connect_to_db()
        data = pd.read_sql(sql_query, conn)
        conn.close()
        return data
    except Exception as exc:
        print(f"Error fetching data: {exc}")
        return pd.DataFrame()


# ===========================================================================
# 2. HELPERS
# ===========================================================================

def _count(start_date: str, end_date: str) -> tuple[int, int]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end   = datetime.strptime(end_date,   "%Y-%m-%d").date()
    weekday_count = weekend_count = 0
    current = start
    while current <= end:
        if current.weekday() < 5:
            weekday_count += 1
        else:
            weekend_count += 1
        current += timedelta(days=1)
    return weekday_count, weekend_count


def _safe_quantile(series: pd.Series, q: float) -> float:
    return float(series.quantile(q)) if not series.empty else 0.0


def _safe_mean(series: pd.Series) -> float:
    return float(series.mean()) if not series.empty else 0.0


def _safe_max(series: pd.Series) -> float:
    return float(series.max()) if not series.empty else 0.0


def _hours_in_range(start_date: str, end_date: str) -> int:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end   = datetime.strptime(end_date,   "%Y-%m-%d").date()
    return ((end - start).days + 1) * 24


def _action_label(current_sku: str, recommended_sku: str, ordered_tiers: list[str]) -> str:
    current_idx     = tier_index(current_sku,     ordered_tiers)
    recommended_idx = tier_index(recommended_sku, ordered_tiers)
    if current_idx == -1 or recommended_idx == -1:
        return "Unknown"
    if recommended_idx < current_idx:
        return "Downsize"
    if recommended_idx > current_idx:
        return "Upsize"
    return "NoChange"


# ===========================================================================
# 3. METACONFIG
# ===========================================================================

def load_metaconfig(provider: str, region: str) -> tuple[dict, list[str]]:
    """
    Load SKU specs from MetaConfig.
    Key by Instance (matches InstanceSize in aggregated table — same as aggregation proc).
    Excludes Free and Flex tiers.
    """
    sql = f"""
        SELECT
            Instance,
            Tier,
            vCores,
            MemorySizeGB,
            CostPrHour,
            ConnectionLimit
        FROM [Analytics].[MongoDBMetaConfig]
        WHERE Provider = '{provider}'
          AND Region   = '{region}'
          AND Tier NOT IN ('Free', 'Flex')
    """
    df = fetch_data(sql)
    if df.empty:
        return {}, []

    df["ConnectionLimit"] = pd.to_numeric(df["ConnectionLimit"], errors="coerce")
    df["CostPrHour"]      = pd.to_numeric(df["CostPrHour"],      errors="coerce")
    df["MemorySizeGB"]    = pd.to_numeric(df["MemorySizeGB"],    errors="coerce")
    df["vCores"]          = pd.to_numeric(df["vCores"],          errors="coerce")

    # Deduplicate: keep best row per Instance (highest ConnectionLimit, lowest cost)
    df = df.sort_values(
        by=["Instance", "ConnectionLimit", "CostPrHour"],
        ascending=[True, False, True]
    )
    df = df.drop_duplicates(subset=["Instance"], keep="first")

    specs = {}
    for _, row in df.iterrows():
        sku = str(row["Instance"]).upper().strip()
        specs[sku] = {
            "RAM_GB":          float(row["MemorySizeGB"])    if pd.notna(row["MemorySizeGB"])    else 0.0,
            "vCPUs":           float(row["vCores"])          if pd.notna(row["vCores"])          else 0.0,
            "ConnectionLimit": float(row["ConnectionLimit"]) if pd.notna(row["ConnectionLimit"]) else 0.0,
            "CostPrHour":      float(row["CostPrHour"])      if pd.notna(row["CostPrHour"])      else 0.0,
            "Tier":            str(row["Tier"]).strip()      if pd.notna(row["Tier"])            else "",
        }

    # ordered_tiers: exclude Low-CPU only (Low-CPU handled separately)
    # Includes Standard, NVMe and any other sizing tier
    ordered = sorted(
        [s for s in specs if "LOW-CPU" not in specs[s].get("Tier", "").upper()],
        key=lambda sku: (specs[sku]["RAM_GB"], specs[sku]["vCPUs"])
    )
    return specs, ordered


def tier_index(sku: str, ordered_tiers: list[str]) -> int:
    try:
        return ordered_tiers.index(str(sku).upper())
    except ValueError:
        return -1


def find_low_cpu_sku(standard_sku: str, specs: dict, provider: str, region: str) -> str | None:
    """
    Find cheapest Low-CPU SKU with same RAM as standard_sku.
    specs is already filtered by provider/region from SQL — no extra check needed.
    """
    rec_ram = specs.get(standard_sku, {}).get("RAM_GB", 0)
    low_cpu_skus = [
        s for s in specs
        if  specs[s].get("RAM_GB")           == rec_ram
        and specs[s].get("Tier", "").upper() == "LOW-CPU"
    ]
    if not low_cpu_skus:
        return None
    return min(low_cpu_skus, key=lambda s: specs[s].get("CostPrHour", 0.0))


def find_nvme_sku(standard_sku: str, specs: dict) -> str | None:
    """
    Find NVMe SKU with same RAM as standard_sku.
    Used when connections are high — NVMe handles more IOPS/connections.
    specs is already filtered by provider/region from SQL.
    """
    rec_ram = specs.get(standard_sku, {}).get("RAM_GB", 0)
    nvme_skus = [
        s for s in specs
        if  specs[s].get("RAM_GB")           == rec_ram
        and "NVME" in specs[s].get("Tier", "").upper()
    ]
    if not nvme_skus:
        return None
    return min(nvme_skus, key=lambda s: specs[s].get("CostPrHour", 0.0))


# ===========================================================================
# 4. TREND + SEASONALITY
# ===========================================================================

def _trend(df: pd.DataFrame, feature: str) -> list[str]:
    if df.empty or feature not in df.columns:
        return ["No trend"]
    weekly = df.resample("W").quantile(0.95)
    if len(weekly) < 2:
        return ["No trend"]
    trend_status = []
    for i in range(len(weekly) - 1):
        pair  = weekly.iloc[i : i + 2][feature].fillna(0)
        y     = pair.values
        x     = np.arange(len(y))
        if len(y) < 2 or np.all(np.isnan(y)):
            trend_status.append("No trend")
            continue
        slope = np.polyfit(x, y, 1)[0]
        if slope > 0:
            trend_status.append("Increasing")
        elif slope < 0:
            trend_status.append("Decreasing")
        else:
            trend_status.append("No trend")
    return trend_status


def _build_hour_range(date_range: pd.DatetimeIndex, hr_type: str, business_hour: str, on_start: int, off_end: int) -> pd.DatetimeIndex:
    if hr_type == "Weekday" and business_hour == "BusinessHours":
        return date_range[(date_range.weekday < 5) & (date_range.hour >= on_start) & (date_range.hour <= off_end)]
    if hr_type == "Weekday" and business_hour == "NonBusinessHours":
        return date_range[(date_range.weekday < 5) & ((date_range.hour < on_start) | (date_range.hour > off_end))]
    return date_range[date_range.weekday >= 5]


def _onseasonality(seasonality: pd.DataFrame, start_date: str, end_date: str, sku: str, hr_type: str, business_hour: str, on_start: int, off_end: int) -> str:
    x = seasonality.groupby(["Date", "Hour", "Action"]).size().reset_index(name="_count")
    x["_datetime"] = pd.to_datetime(x["Date"]) + pd.to_timedelta(x["Hour"], unit="h")
    if x[x["Action"] == sku].empty:
        return "Empty"
    date_range = pd.date_range(start=start_date, end=f"{end_date} 23:59:00", freq="h")
    hrs_range  = _build_hour_range(date_range, hr_type, business_hour, on_start, off_end)
    df_l1 = x[x["Action"] == sku].set_index("_datetime").reindex(hrs_range, fill_value=0).reset_index()
    df_l1.columns = ["_datetime", "Date", "Hour", "Action", "_count"]
    df_l1 = df_l1[["_datetime", "_count"]].set_index("_datetime")
    hourly = int((df_l1.groupby(df_l1.index.hour).quantile(0.9)["_count"] >= 1).any())
    daily  = int((df_l1.groupby(df_l1.index.date).quantile(0.9)["_count"].sum() > 1))
    if hourly and daily:
        return "Seasonality : Hourly,Daily"
    if hourly:
        return "Seasonality : Hourly"
    if daily:
        return "Seasonality : Daily"
    return "No Seasonality : Daily or Hourly"


# ===========================================================================
# 5. CLUSTERING
# ===========================================================================

def perform_clustering_and_rightsizing(metric_type: str, data: pd.DataFrame) -> pd.DataFrame:
    df    = data.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df["Week"] = df["Date"].dt.isocalendar().week
    mt     = metric_type
    result = []

    for week, group in df.groupby("Week"):
        if len(group) < 3:
            continue
        # Level 2 check — simulate 2 step downsize (multiply by 4)
        # All 3 metrics must stay below 100% after x4
        level2 = group[[f"{mt}MaxP95", f"{mt}Max", f"{mt}AvgP95"]].mul(4)
        level2.columns = [f"{mt}MaxP952level", f"{mt}Max2level", f"{mt}AvgP952level"]
        k2 = KMeans(n_clusters=3, random_state=42)
        level2["Cluster"] = k2.fit_predict(level2)
        c2 = level2.groupby("Cluster").quantile(0.95).reset_index()
        c2["Week"] = week

        # Level 1 check — simulate 1 step downsize (multiply by 2)
        # Postgres pattern: only CpuMaxP95 and CpuAvgP95
        # CpuMax NOT included here (causes all downsizes to fail)
        # CpuMax only used in level2 (×4) check
        level1 = group[[f"{mt}MaxP95", f"{mt}AvgP95"]].mul(2)
        level1.columns = [f"{mt}MaxP951level", f"{mt}AvgP951level"]
        k1 = KMeans(n_clusters=3, random_state=42)
        level1["Cluster"] = k1.fit_predict(level1)
        c1 = level1.groupby("Cluster").quantile(0.95).reset_index()
        c1["Week"] = week

        merged = pd.merge(c1, c2, on=["Cluster", "Week"])
        result.append(merged)

    if not result:
        return pd.DataFrame(columns=["Week", "Action1"])

    final_df = pd.concat(result, ignore_index=True)

    def assign_cluster(row: pd.Series) -> int:
        # Level 2: can downsize 2 steps?
        # CpuMaxP95 × 4, CpuMax × 4, CpuAvgP95 × 4
        # ALL must be < 100%
        c2_ok = (
            row.get(f"{mt}MaxP952level", 101) < 100
            and row.get(f"{mt}Max2level",    101) < 100
            and row.get(f"{mt}AvgP952level", 101) < 100
        )
        # Level 1: can downsize 1 step?
        # Exact Postgres pattern:
        # Only CpuMaxP95 × 2 and CpuAvgP95 × 2
        # Both must be < 100%
        c1_ok = (
            row.get(f"{mt}MaxP951level", 101) < 100
            and row.get(f"{mt}AvgP951level", 101) < 100
        )
        if c2_ok:
            return 1
        if c1_ok:
            return 2
        return 3

    final_df["Action1"] = final_df.apply(assign_cluster, axis=1)
    return final_df


# ===========================================================================
# 6. RECOMMENDATIONS
# ===========================================================================

def recommendations(metric_type: str, data: pd.DataFrame, actual_sku: str, ordered_tiers: list[str]) -> list | None:
    if data.empty:
        return None

    mt = metric_type
    df = data.copy()

    if mt == "Mem":
        if "MemMaxGt50" not in df.columns:
            df["MemMaxGt50"] = (df["MemResidentMaxPct"] > 50).astype(int)
        if "MemMaxGt25" not in df.columns:
            df["MemMaxGt25"] = (df["MemResidentMaxPct"] > 25).astype(int)
        df["MemMax"]    = df["MemResidentMaxPct"]
        df["MemMaxP95"] = df.get("MemResidentP95Pct", df["MemResidentMaxPct"])
        df["MemAvgP95"] = df.get("MemResidentAvgPct", df["MemResidentMaxPct"])

    gt50 = f"{mt}MaxGt50"
    gt25 = f"{mt}MaxGt25"

    for col in [gt50, gt25]:
        if col not in df.columns:
            df[col] = 0

    df.loc[
        (df[f"{mt}MaxP95"] <= 25)
        & (df[f"{mt}AvgP95"] <= 25)
        & (df[gt50] == 0)
        & (df[gt25] == 0)
        & (df["InstanceSize"] == actual_sku),
        "Action",
    ] = "L1"

    df.loc[
        (df[f"{mt}MaxP95"] > 50)
        & ((df[gt50] >= 1) | (df[gt25] > 2))
        & (df["InstanceSize"] == actual_sku),
        "Action",
    ] = "L3"

    df.loc[df["InstanceSize"] != actual_sku, "Action"] = "L3"
    df["Action"] = df["Action"].fillna("L2")

    weekly = perform_clustering_and_rightsizing(mt, df)
    if weekly.empty:
        return [(actual_sku, 0, "Insufficient data for clustering")]

    trend_df = df.copy()
    trend_df["_datetime"] = pd.to_datetime(
        trend_df["Date"].astype(str) + " " + trend_df["Hour"].astype(str) + ":00:00"
    )
    trend_df.set_index("_datetime", inplace=True)
    trend_status = _trend(trend_df[[f"{mt}MaxP95", f"{mt}AvgP95", gt50]], gt50)

    current_idx = tier_index(actual_sku, ordered_tiers)
    if current_idx == -1:
        return [(actual_sku, 0, "Unknown tier")]

    # Peak value: use P95 of P95 values — avoids single spike triggering ScaleUp
    peak_val = _safe_quantile(df[f"{mt}MaxP95"], 0.95) if f"{mt}MaxP95" in df.columns else 0.0

    weekly_actions = []
    for i, (_, row) in enumerate(weekly.iterrows()):
        a1 = int(row["Action1"])
        ts = trend_status[i - 1] if i > 0 and i - 1 < len(trend_status) else "No trend"

        if ts == "Increasing" or a1 == 3:
            if peak_val > 80:   # Genuinely high load → ScaleUp
                target_idx = min(len(ordered_tiers) - 1, current_idx + 1)
            else:               # Small trend on low values → stay current
                target_idx = current_idx
            status = 3
        elif a1 == 1:
            target_idx = max(0, current_idx - 2)
            status = 1
        else:
            target_idx = max(0, current_idx - 1)
            status = 2

        weekly_actions.append((status, ordered_tiers[target_idx]))

    if any(s == 3 for s, _ in weekly_actions) and peak_val > 80:
        final_idx = min(len(ordered_tiers) - 1, current_idx + 1)  # Upsize 1 step
    elif all(s == 1 for s, _ in weekly_actions):
        final_idx = max(0, current_idx - 1)  # Downsize 1 step only (conservative)
    else:
        final_idx = max(0, current_idx - 1)  # Downsize 1 step

    return [(ordered_tiers[final_idx], final_idx, "")]


def connections_recommendation(data: pd.DataFrame, actual_sku: str, ordered_tiers: list[str], specs: dict) -> tuple[str, str]:
    if data.empty:
        return actual_sku, ""
    max_conn_pct = _safe_quantile(data["ConnUtilizationPct"], 0.95) if "ConnUtilizationPct" in data.columns else 0.0
    current_idx  = tier_index(actual_sku, ordered_tiers)
    if current_idx == -1:
        return actual_sku, ""
    if max_conn_pct < 35:
        return ordered_tiers[max(0, current_idx - 1)], "Connections underutilized"
    if max_conn_pct > 80:
        return ordered_tiers[min(len(ordered_tiers) - 1, current_idx + 1)], "Connections intensive"
    return actual_sku, ""


def component_comment(cpu_idx: int, mem_idx: int, conn_idx: int, current_idx: int) -> str:
    """
    val > current_idx → Intensive    (needs bigger SKU)
    val < current_idx → Underutilized (can use smaller SKU)
    val == current_idx → Optimal     (current SKU is right)
    """
    components = {"CPU": cpu_idx, "Memory": mem_idx, "Connections": conn_idx}
    intensive, underutilized, optimal = [], [], []
    for name, val in components.items():
        if val > current_idx:
            intensive.append(name)
        elif val < current_idx:
            underutilized.append(name)
        else:
            optimal.append(name)
    parts = []
    if intensive:
        parts.append(", ".join(intensive) + " Intensive")
    if underutilized:
        parts.append(", ".join(underutilized) + " Underutilized")
    if optimal:
        parts.append(", ".join(optimal) + " Optimal")
    return " ; ".join(parts)


# ===========================================================================
# 7. SQL BUILDERS
# ===========================================================================

def load_spend_data(month: str) -> dict:
    """
    Load actual monthly spend per cluster from MongoDB.Spend table.
    Returns dict: {ClusterName: ActualMonthlySpend}
    Uses SUM(Amount) for the given month.
    More accurate than CostPrHour × hours calculation.
    """
    sql = f"""
        SELECT
            Cluster                     AS ClusterName,
            ROUND(SUM(Amount), 2)       AS ActualSpend
        FROM [MongoDB].[Spend]
        WHERE FORMAT(CAST(UsageDate AS DATE), 'yyyy-MM') = '{month}'
        GROUP BY Cluster
    """
    df = fetch_data(sql)
    if df.empty:
        return {}
    return dict(zip(df["ClusterName"], df["ActualSpend"]))


def build_cluster_inventory_query(start_date: str, end_date: str) -> str:
    return f"""
        -- Use MongoDB.Clusters as source of truth
        -- Ensures new clusters are included
        -- Ensures decommissioned clusters excluded
        -- StateName IN ('IDLE','UPDATING') = active only
        -- Paused = 0 = not paused
        -- Join Aggregated for InstanceSize/Provider/Region
        SELECT DISTINCT
            c.ClustersKey                           AS ClusterKey,
            c.Name                                  AS ClusterName,
            a.InstanceSize,
            a.ProviderName,
            a.RegionName,
            a.OrgKey,
            CAST(a.OrgKey AS NVARCHAR(255))         AS OrgName,
            c.ProjectKey
        FROM [MongoDB].[Clusters] c
        INNER JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
            ON  a.ClusterKey = c.ClustersKey
            AND a._date BETWEEN '{start_date}' AND '{end_date}'
        WHERE c.StateName IN ('IDLE', 'UPDATING')
          AND c.Paused      = 0
          AND a.InstanceSize IS NOT NULL
    """


def build_cluster_metrics_query(cluster_key: int, start_date: str, end_date: str,
                                 day_type: str, business_hour: str, business_hour1: str) -> str:
    return f"""
        SELECT
            ClusterKey, ClusterName, InstanceSize,
            ProviderName, RegionName,
            _date  AS [Date],
            _hour  AS [Hour],
            [type] AS [DayType],
            CASE WHEN [type] = 'Weekend' THEN 'Weekend' ELSE businessHour END AS HourType,
            MAX(CpuAvg)             AS CpuAvg,
            MAX(CpuAvgP95)          AS CpuAvgP95,
            MAX(CpuMax)             AS CpuMax,
            MAX(CpuMaxP95)          AS CpuMaxP95,
            MAX(CpuMaxGt50)         AS CpuMaxGt50,
            MAX(CpuMaxGt25)         AS CpuMaxGt25,
            MAX(CpuMaxGt10)         AS CpuMaxGt10,
            MAX(MemResidentMax)     AS MemResidentMax,
            MAX(MemResidentAvg)     AS MemResidentAvg,
            MAX(MemResidentMaxPct)  AS MemResidentMaxPct,
            MAX(MemResidentAvgPct)  AS MemResidentAvgPct,
            MAX(MemResidentP95Pct)  AS MemResidentP95Pct,
            MAX(NetInAvg)           AS NetInAvg,
            MAX(NetInMax)           AS NetInMax,
            MAX(NetOutAvg)          AS NetOutAvg,
            MAX(NetOutMax)          AS NetOutMax,
            MAX(NetRequestsMax)     AS NetRequestsMax,
            SUM(ConnectionsMax)     AS ConnectionsMax,
            SUM(ConnectionsAvg)     AS ConnectionsAvg,
            MAX(ConnUtilizationPct) AS ConnUtilizationPct,
            MAX(OpcQueryMax)        AS OpcQueryMax,
            MAX(OpcInsertMax)       AS OpcInsertMax
        FROM [Metrics].[MongoDBRightsizingAggregated5Min]
        WHERE _date BETWEEN '{start_date}' AND '{end_date}'
          AND ClusterKey = {cluster_key}
          AND [type] IN ('{day_type}')
          AND (businessHour IN ('{business_hour}') OR businessHour IN ('{business_hour1}'))
        GROUP BY
            ClusterKey, ClusterName, InstanceSize, ProviderName, RegionName,
            _date, _hour, [type],
            CASE WHEN [type] = 'Weekend' THEN 'Weekend' ELSE businessHour END
        ORDER BY _date, _hour
    """


# ===========================================================================
# 8. EFFICIENCY CALCULATION
# ===========================================================================

def calculate_efficiency(data: pd.DataFrame, actual_sku: str, recommended_sku: str, specs: dict) -> tuple:
    avg_cpu  = _safe_mean(data["CpuAvg"])             if "CpuAvg"             in data.columns else 0.0
    max_cpu  = _safe_max(data["CpuMax"])              if "CpuMax"             in data.columns else 0.0
    avg_mem  = _safe_mean(data["MemResidentAvgPct"])  if "MemResidentAvgPct"  in data.columns else 0.0
    max_mem  = _safe_max(data["MemResidentMaxPct"])   if "MemResidentMaxPct"  in data.columns else 0.0
    avg_conn = _safe_mean(data["ConnUtilizationPct"]) if "ConnUtilizationPct" in data.columns else 0.0

    current_efficiency = json.dumps({
        "CpuAvgPct": round(avg_cpu,  2),
        "CpuMaxPct": round(max_cpu,  2),
        "MemAvgPct": round(avg_mem,  2),
        "MemMaxPct": round(max_mem,  2),
        "ConnPct":   round(avg_conn, 2),
    })

    current_ram  = specs.get(actual_sku,      {}).get("RAM_GB",          0.0)
    rec_ram      = specs.get(recommended_sku, {}).get("RAM_GB",          0.0)
    current_conn = specs.get(actual_sku,      {}).get("ConnectionLimit",  0.0)
    rec_conn     = specs.get(recommended_sku, {}).get("ConnectionLimit",  0.0)
    current_vcpu = specs.get(actual_sku,      {}).get("vCPUs",            0.0)
    rec_vcpu     = specs.get(recommended_sku, {}).get("vCPUs",            0.0)

    proj_cpu_avg = round((avg_cpu  * current_vcpu / rec_vcpu) if rec_vcpu > 0 else avg_cpu, 2)
    proj_cpu_max = round((max_cpu  * current_vcpu / rec_vcpu) if rec_vcpu > 0 else max_cpu, 2)
    proj_mem     = round((avg_mem  * current_ram  / rec_ram)  if rec_ram  > 0 else 0.0,     2)
    proj_conn    = round((avg_conn * current_conn / rec_conn) if rec_conn > 0 else 0.0,     2)

    within_efficiency = json.dumps({
        "ProjectedCpuAvgPct": proj_cpu_avg,
        "ProjectedCpuMaxPct": proj_cpu_max,
        "ProjectedMemPct":    proj_mem,
        "ProjectedConnPct":   proj_conn,
        "RecommendedSku":     recommended_sku,
    })

    return current_efficiency, within_efficiency


# ===========================================================================
# 9. MAIN PER-CLUSTER PIPELINE
# ===========================================================================

def process_cluster(cluster_key: int, cluster_name: str, instance_size: str,
                    provider_name: str, region_name: str, config: dict,
                    metacache: dict, spend_data: dict = {}) -> dict | None:

    start_date = config["StartDate"]
    end_date   = config["EndDate"]
    day_type   = config["Type"]
    bh         = config["BusinessHour"]
    bh1        = config["BusinessHour1"]

    meta_key = (str(provider_name).upper(), str(region_name).upper())
    if meta_key not in metacache:
        print(f"Loading MetaConfig for Provider={provider_name}, Region={region_name}")
        metacache[meta_key] = load_metaconfig(meta_key[0], meta_key[1])
        specs, ordered = metacache[meta_key]
        
    specs, ordered_tiers = metacache[meta_key]

    if not ordered_tiers:
        print(f"Skipping {cluster_name} — no MetaConfig for {provider_name}/{region_name}")
        return None

    actual_sku = str(instance_size).upper()
    if actual_sku not in ordered_tiers:
        print(f"Skipping {cluster_name} — SKU {actual_sku} not in MetaConfig")
        return None

    # Filter ordered_tiers to same tier as current SKU
    # NVMe cluster → NVMe options only | Standard cluster → Standard options only
    current_tier = specs.get(actual_sku, {}).get("Tier", "").upper()
    if current_tier:
        same_tier = [s for s in ordered_tiers if specs[s].get("Tier", "").upper() == current_tier]
        if len(same_tier) >= 2:
            ordered_tiers = same_tier
    if actual_sku not in ordered_tiers:
        print(f"Skipping {cluster_name} — SKU {actual_sku} has no same-tier options")
        return None

    sql  = build_cluster_metrics_query(cluster_key, start_date, end_date, day_type, bh, bh1)
    data = fetch_data(sql)
    if data.empty:
        print(f"Skipping {cluster_name} — no metrics found")
        return None

    data["InstanceSize"] = actual_sku

    cpu_rec = recommendations("Cpu", data, actual_sku, ordered_tiers)

    # Memory flag — skip memory recommendations for ALL clusters
    # Set SKIP_MEMORY_RECOMMENDATIONS = False to re-enable
    if SKIP_MEMORY_RECOMMENDATIONS:
        mem_rec = []
    else:
        mem_rec = recommendations("Mem", data, actual_sku, ordered_tiers)

    conn_sku, conn_comment = connections_recommendation(data, actual_sku, ordered_tiers, specs)

    cpu_sku = cpu_rec[0][0] if cpu_rec else actual_sku
    mem_sku = mem_rec[0][0] if mem_rec else actual_sku

    current_idx = tier_index(actual_sku, ordered_tiers)
    cpu_idx     = tier_index(cpu_sku,    ordered_tiers)
    mem_idx     = tier_index(mem_sku,    ordered_tiers)
    conn_idx    = tier_index(conn_sku,   ordered_tiers)

    overall_idx = max(cpu_idx, mem_idx, conn_idx)
    overall_sku = ordered_tiers[overall_idx]

    # Connection-only upsize → NoChange
    # Connections can be fixed from app side (connection pooling)
    # Suggest NVMe alternative for better IOPS/connection handling
    conn_only_upsize = (conn_idx > current_idx and cpu_idx <= current_idx and mem_idx <= current_idx)
    if conn_only_upsize:
        overall_idx = current_idx
        overall_sku = actual_sku
        print(f"Connection-only upsize skipped for {cluster_name}")

    current_cost              = specs.get(actual_sku,  {}).get("CostPrHour", 0.0)
    recommended_cost          = specs.get(overall_sku, {}).get("CostPrHour", 0.0)
    hours_in_month            = _hours_in_range(start_date, end_date)

    # Use actual spend from MongoDB.Spend table
    # Falls back to calculated spend if not available
    spend_30_days             = spend_data.get(cluster_name,
                                current_cost * hours_in_month)
    estimated_monthly_savings = (current_cost - recommended_cost) * hours_in_month

    comment = "High Connections — Review Connection Pooling" if conn_only_upsize \
              else component_comment(cpu_idx, mem_idx, conn_idx, current_idx)

    misc_comment = (
        f"CPU:{cpu_rec[0][2] if cpu_rec else ''} / "
        f"Mem:{mem_rec[0][2] if mem_rec else ''} / "
        f"Connections:{conn_comment}"
    )

    hour_type            = "Weekend" if day_type == "Weekend" else bh
    avg_cpu_max          = _safe_mean(data["CpuMax"])                      if "CpuMax"             in data.columns else 0.0
    peak_cpu_max         = _safe_max(data["CpuMax"])                       if "CpuMax"             in data.columns else 0.0
    mem_utilization_pct  = _safe_quantile(data["MemResidentMaxPct"], 0.95) if "MemResidentMaxPct"  in data.columns else 0.0
    conn_utilization_pct = _safe_quantile(data["ConnUtilizationPct"], 0.95) if "ConnUtilizationPct" in data.columns else 0.0
    action               = _action_label(actual_sku, overall_sku, ordered_tiers)
    audit_utc            = datetime.now(timezone.utc).replace(tzinfo=None)

    # Low-CPU alternative (for Downsize / Upsize)
    prov         = str(provider_name).upper()
    reg          = str(region_name).upper()
    low_cpu_sku  = None
    nvme_sku     = None

    if conn_only_upsize:
        # Connection-only → suggest NVMe (better IOPS) instead of Low-CPU
        nvme_sku = find_nvme_sku(overall_sku, specs)
        recommended_sku_display = f"{overall_sku}, {nvme_sku}" if nvme_sku else overall_sku
        low_cpu_savings = 0.0
    else:
        # Normal Downsize / Upsize → suggest Low-CPU alternative
        low_cpu_sku  = find_low_cpu_sku(overall_sku, specs, prov, reg)
        low_cpu_cost = specs.get(low_cpu_sku, {}).get("CostPrHour", 0.0) if low_cpu_sku else 0.0

        # Skip Low-CPU if peak CPU is high (fewer vCores would be risky)
        if low_cpu_sku and peak_cpu_max > 50:
            low_cpu_sku  = None
            low_cpu_cost = 0.0

        low_cpu_savings = round(
            (current_cost - low_cpu_cost) * hours_in_month, 2
        ) if low_cpu_sku and low_cpu_cost < current_cost else 0.0

        recommended_sku_display = f"{overall_sku}, {low_cpu_sku}" if low_cpu_sku else overall_sku

    eff = calculate_efficiency(data, actual_sku, overall_sku, specs)

    return {
        "ClusterKey":              cluster_key,
        "ClusterName":             cluster_name,
        "DayType":                 day_type,
        "HourType":                hour_type,
        "CurrentSku":              actual_sku,
        "CurrentCostPrHour":       current_cost,
        "CpuRec":                  cpu_sku,
        "MemRec":                  mem_sku,
        "ConnRec":                 conn_sku,
        "AvgCpuMax":               avg_cpu_max,
        "PeakCpuMax":              peak_cpu_max,
        "MemUtilizationPct":       mem_utilization_pct,
        "ConnUtilizationPct":      conn_utilization_pct,
        "RecommendedSku":          recommended_sku_display,
        "RecommendedCostPrHour":   recommended_cost,
        "EstimatedMonthlySavings": estimated_monthly_savings,
        "Comment":                 comment,
        "MiscComment":             misc_comment,
        "CurrentEfficiency":       eff[0],
        "WithinEfficiency":        eff[1],
        "LowCpuEfficiency":        None,          # populated by usp_MongoDBRightsizingEfficiency
        "Spend30days":             spend_30_days,
        "WithinFamilySavings":     estimated_monthly_savings,
        "LowCpuSku":               low_cpu_sku,
        "LowCpuSavings":           low_cpu_savings,
        "Action":                  action,
        "AuditUtc":                audit_utc,
    }


# ===========================================================================
# 10. OUTPUT INSERT
# ===========================================================================

INSERT_SQL = """
    INSERT INTO [Metrics].[MongoDBRightsizingRecommendations] (
        Month, ClusterKey, ClusterName, OrgName, ProjectKey,
        ProviderName, RegionName, DayType, HourType,
        CurrentSku, CurrentCostPrHour,
        CpuRec, MemRec, ConnRec, AvgCpuMax, PeakCpuMax,
        MemUtilizationPct, ConnUtilizationPct, RecommendedSku,
        RecommendedCostPrHour, EstimatedMonthlySavings, Comment,
        MiscComment, CurrentEfficiency, WithinEfficiency,
        LowCpuEfficiency, Spend30days, WithinFamilySavings,
        LowCpuSku, LowCpuSavings, Action, AuditUtc
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_COLUMNS = [
    "Month", "ClusterKey", "ClusterName", "OrgName", "ProjectKey",
    "ProviderName", "RegionName", "DayType", "HourType",
    "CurrentSku", "CurrentCostPrHour",
    "CpuRec", "MemRec", "ConnRec", "AvgCpuMax", "PeakCpuMax",
    "MemUtilizationPct", "ConnUtilizationPct", "RecommendedSku",
    "RecommendedCostPrHour", "EstimatedMonthlySavings", "Comment",
    "MiscComment", "CurrentEfficiency", "WithinEfficiency",
    "LowCpuEfficiency", "Spend30days", "WithinFamilySavings",
    "LowCpuSku", "LowCpuSavings", "Action", "AuditUtc",
]


def call_stored_proc(proc_name: str, month: str):
    """Call stored procedure with @LastMonth — autocommit required for Synapse DDL."""
    try:
        conn = connect_to_db()
        conn.autocommit = True    # DDL inside proc (DROP/CREATE TABLE) needs no transaction
        cur  = conn.cursor()
        print(f"Running for month: {month}")
        cur.execute(f"EXEC {proc_name} @LastMonth = ?", month)
        cur.close()
        conn.close()
        print(f"Stored procedure executed successfully.")
    except Exception as exc:
        print(f"Error executing stored procedures: {exc}")
        raise


def upsert_in_chunks(df: pd.DataFrame, chunk_size: int = 100):
    total = len(df)
    print(f"Upserting {total} rows...")

    # Step 1: Delete ALL existing rows for this month
    # Done ONCE before insert to prevent duplicates
    # Handles case where script runs multiple times
    months_in_df = df["Month"].unique().tolist()
    conn = connect_to_db()
    cur  = conn.cursor()
    for month in months_in_df:
        cluster_keys = df[df["Month"] == month]["ClusterKey"].unique().tolist()
        for ck in cluster_keys:
            cur.execute("""
                DELETE FROM [Metrics].[MongoDBRightsizingRecommendations]
                WHERE Month      = ?
                AND   ClusterKey = ?
            """, (month, int(ck)))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Deleted existing rows for months: {months_in_df}")

    # Step 2: Insert all fresh rows in chunks
    for start in range(0, total, chunk_size):
        chunk = df.iloc[start : start + chunk_size].copy()
        conn  = connect_to_db()
        cur   = conn.cursor()

        for _, row in chunk.iterrows():
            cur.execute(INSERT_SQL, tuple(row[col] for col in INSERT_COLUMNS))

        conn.commit()
        cur.close()
        conn.close()
        print(f"{min(start + chunk_size, total)}/{total} rows upserted")

    print("Upsert complete.")


# ===========================================================================
# 11. MAIN
# ===========================================================================

if __name__ == "__main__":
    # Previous complete month (Postgres pattern)
    # Running June 3rd → uses May 2026 full month
    # Running July 3rd → uses June 2026 full month
    today           = datetime.now()
    last_month_date = today - relativedelta(months=1)

    months      = [last_month_date.strftime("%Y-%m")]
    start_dates = [last_month_date.replace(day=1)
                   .strftime("%Y-%m-%d")]
    end_dates   = [((last_month_date.replace(day=1)
                   + relativedelta(months=1))
                   - timedelta(days=1)).strftime("%Y-%m-%d")]

    print("months =", months)
    print("StartDate =", start_dates[0])
    print("EndDate =", end_dates[0])
    print("Memory recommendations =", "SKIPPED (flag enabled)" if SKIP_MEMORY_RECOMMENDATIONS else "ENABLED")

    types           = ["Weekday",       "Weekday",           "Weekend"]
    business_hours  = ["BusinessHours", "NonBusinessHours",  "Weekend"]
    business_hours1 = ["BusinessHours", "NonBusinessHours",  "Weekend"]

    month_dict = {}
    for month, start, end in zip(months, start_dates, end_dates):
        month_dict[month] = [
            {"Type": t, "BusinessHour": bh, "BusinessHour1": bh1, "StartDate": start, "EndDate": end}
            for t, bh, bh1 in zip(types, business_hours, business_hours1)
        ]

    out_df    = pd.DataFrame(columns=INSERT_COLUMNS)
    metacache = {}

    for month, configs in month_dict.items():
        print(f"Running for month: {month}")

        # Load actual spend from MongoDB.Spend table
        spend_data = load_spend_data(month)
        print(f"Spend data loaded: {len(spend_data)} clusters")

        for config in configs:
            _count(config["StartDate"], config["EndDate"])

            clusters = fetch_data(build_cluster_inventory_query(config["StartDate"], config["EndDate"]))
            if clusters.empty:
                continue

            for _, row in clusters.iterrows():
                cluster_key   = int(row["ClusterKey"])
                cluster_name  = str(row["ClusterName"])
                instance_size = str(row["InstanceSize"])
                provider_name = str(row.get("ProviderName", ""))
                region_name   = str(row.get("RegionName",   ""))
                org_key       = row.get("OrgKey",      "")
                project_key   = row.get("ProjectKey",  "")

                print(f"{cluster_name}")
                result = process_cluster(
                    cluster_key, cluster_name, instance_size,
                    provider_name, region_name, config, metacache,
                    spend_data,
                )
                if result is None:
                    continue

                result["Month"]        = month
                result["OrgName"]      = row.get("OrgName") or org_key
                result["ProjectKey"]   = project_key
                result["ProviderName"] = provider_name
                result["RegionName"]   = region_name

                out_df = pd.concat([out_df, pd.DataFrame([result])], ignore_index=True)

    if not out_df.empty:
        print(f"Total recommendations: {len(out_df)}")

        # Step 1: Upsert recommendations
        upsert_in_chunks(out_df)

        # Step 2: Simulated metrics proc
        call_stored_proc("[Metrics].[usp_MongoDBRightsizingSimulatedMetrics]", months[0])

        # Step 3: Efficiency proc
        call_stored_proc("[Metrics].[usp_MongoDBRightsizingEfficiency]", months[0])

        print("All stored procedures executed successfully.")
    else:
        print("No recommendations generated.")