"""
MongoDB Atlas Rightsizing Engine (5-Min Aggregated Metrics)
===========================================================
Uses [Metrics].[MongoDBRightsizingAggregated5Min] as source.
Implements PostgreSQL-like pattern:
- per-slice evaluation (weekday business / weekday non-business / weekend)
- action labeling (L1/L2/L3)
- weekly clustering
- trend + seasonality
- one recommendation per cluster
"""

import re
import warnings
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pyodbc
from dateutil.relativedelta import relativedelta
from sklearn.cluster import KMeans


warnings.filterwarnings("ignore")


# ===========================================================================
# 1. DATABASE CONNECTION
# ===========================================================================

SERVER = "hybridasa.sql.azuresynapse.net"
DATABASE = "hybridasa_dedicatedpool"
USERNAME = "hybridasawrite"
PASSWORD = "H@Sh1CoRS!"
DRIVER = "{ODBC Driver 17 for SQL Server}"


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
        print("[DEBUG] Executing SQL query:\n", sql_query)
        conn = connect_to_db()
        data = pd.read_sql(sql_query, conn)
        print(f"[DEBUG] Query returned {data.shape[0]} rows and {data.shape[1]} columns.")
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
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
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
    if series.empty:
        return 0.0
    return float(series.quantile(q))


def _safe_mean(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    return float(series.mean())


def _safe_max(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    return float(series.max())


def _hours_in_range(start_date: str, end_date: str) -> int:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    return ((end - start).days + 1) * 24


def _action_label(current_sku: str, recommended_sku: str, ordered_tiers: list[str]) -> str:
    current_idx = tier_index(current_sku, ordered_tiers)
    recommended_idx = tier_index(recommended_sku, ordered_tiers)
    if current_idx == -1 or recommended_idx == -1:
        return "Unknown"
    if recommended_idx < current_idx:
        return "Downsize"
    if recommended_idx > current_idx:
        return "Upsize"
    return "NoChange"


# ===========================================================================
# 3. METACONFIG (DYNAMIC TIERS)
# ===========================================================================


def load_metaconfig(provider: str, region: str) -> tuple[dict, list[str]]:
    sql = f"""
        SELECT
            SkuName,
            Tier,
            vCores,
            MemorySizeGB,
            Instance,
            CostPrHour,
            Provider,
            Region,
            ConnectionLimit
        FROM [Analytics].[MongoDBMetaConfig]
        WHERE Provider = '{provider}'
          AND Region = '{region}'
          AND Tier NOT IN ('Free', 'Flex')
    """
    df = fetch_data(sql)
    if df.empty:
        return {}, []

    # Keep a single best row per SkuName (prefer non-null connection limit, lower cost)
    df["ConnectionLimit"] = pd.to_numeric(df["ConnectionLimit"], errors="coerce")
    df["CostPrHour"] = pd.to_numeric(df["CostPrHour"], errors="coerce")
    df["MemorySizeGB"] = pd.to_numeric(df["MemorySizeGB"], errors="coerce")
    df["vCores"] = pd.to_numeric(df["vCores"], errors="coerce")

    df = df.sort_values(by=["SkuName", "ConnectionLimit", "CostPrHour"], ascending=[True, False, True])
    df = df.drop_duplicates(subset=["SkuName"], keep="first")

    specs = {}
    for _, row in df.iterrows():
        sku = str(row["SkuName"]).upper()
        specs[sku] = {
            "RAM_GB": float(row["MemorySizeGB"]) if pd.notna(row["MemorySizeGB"]) else 0.0,
            "vCPUs": float(row["vCores"]) if pd.notna(row["vCores"]) else 0.0,
            "ConnectionLimit": float(row["ConnectionLimit"]) if pd.notna(row["ConnectionLimit"]) else 0.0,
            "CostPrHour": float(row["CostPrHour"]) if pd.notna(row["CostPrHour"]) else 0.0,
        }

    # Tier order: primarily RAM, then vCPU
    ordered = sorted(
        list(specs.keys()),
        key=lambda sku: (specs[sku]["RAM_GB"], specs[sku]["vCPUs"]) 
    )
    return specs, ordered


def tier_index(sku: str, ordered_tiers: list[str]) -> int:
    try:
        return ordered_tiers.index(str(sku).upper())
    except ValueError:
        return -1


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
        pair = weekly.iloc[i : i + 2][feature].fillna(0)
        y = pair.values
        x = np.arange(len(y))
        if len(y) < 2 or np.all(np.isnan(y)):
            trend_status.append("No trend")
            continue
        # Simple linear regression slope
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
    hrs_range = _build_hour_range(date_range, hr_type, business_hour, on_start, off_end)

    df_l1 = x[x["Action"] == sku].set_index("_datetime").reindex(hrs_range, fill_value=0).reset_index()
    df_l1.columns = ["_datetime", "Date", "Hour", "Action", "_count"]
    df_l1 = df_l1[["_datetime", "_count"]].set_index("_datetime")

    hourly = int((df_l1.groupby(df_l1.index.hour).quantile(0.9)["_count"] >= 1).any())
    daily = int((df_l1.groupby(df_l1.index.date).quantile(0.9)["_count"].sum() > 1))

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
    df = data.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df["Week"] = df["Date"].dt.isocalendar().week

    mt = metric_type
    result = []

    for week, group in df.groupby("Week"):
        if len(group) < 3:
            continue

        level2 = group[[f"{mt}MaxP95", f"{mt}Max", f"{mt}AvgP95"]].mul(4)
        level2.columns = [f"{mt}MaxP952level", f"{mt}Max2level", f"{mt}AvgP952level"]
        k2 = KMeans(n_clusters=3, random_state=42)
        level2["Cluster"] = k2.fit_predict(level2)
        c2 = level2.groupby("Cluster").quantile(0.95).reset_index()
        c2["Week"] = week

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
        c2_ok = (
            row.get(f"{mt}MaxP952level", 101) < 100
            and row.get(f"{mt}Max2level", 101) < 100
            and row.get(f"{mt}AvgP952level", 101) < 100
        )
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

    # MongoDB thresholds (aligned to new 5-min proc outputs)
    # For CPU: uses CpuMaxGt50 / CpuMaxGt25
    # For Memory: derive from MemResidentMaxPct when explicit counters do not exist
    if mt == "Mem":
        if "MemMaxGt50" not in df.columns:
            df["MemMaxGt50"] = (df["MemResidentMaxPct"] > 50).astype(int)
        if "MemMaxGt25" not in df.columns:
            df["MemMaxGt25"] = (df["MemResidentMaxPct"] > 25).astype(int)
        df["MemMax"] = df["MemResidentMaxPct"]
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
    trend_df["_datetime"] = pd.to_datetime(trend_df["Date"].astype(str) + " " + trend_df["Hour"].astype(str) + ":00:00")
    trend_df.set_index("_datetime", inplace=True)
    trend_status = _trend(trend_df[[f"{mt}MaxP95", f"{mt}AvgP95", gt50]], gt50)

    current_idx = tier_index(actual_sku, ordered_tiers)
    if current_idx == -1:
        return [(actual_sku, 0, "Unknown tier")]

    weekly_actions = []
    for i, (_, row) in enumerate(weekly.iterrows()):
        a1 = int(row["Action1"])
        ts = trend_status[i - 1] if i > 0 and i - 1 < len(trend_status) else "No trend"

        if ts == "Increasing" or a1 == 3:
            target_idx = current_idx
            status = 3
        elif a1 == 1:
            target_idx = max(0, current_idx - 2)
            status = 1
        else:
            target_idx = max(0, current_idx - 1)
            status = 2

        weekly_actions.append((status, ordered_tiers[target_idx]))

    if any(s == 3 for s, _ in weekly_actions):
        final_idx = current_idx
    elif all(s == 1 for s, _ in weekly_actions):
        final_idx = max(0, current_idx - 2)
    else:
        final_idx = max(0, current_idx - 1)

    final_sku = ordered_tiers[final_idx]
    return [(final_sku, final_idx, "")]


def connections_recommendation(data: pd.DataFrame, actual_sku: str, ordered_tiers: list[str], specs: dict) -> tuple[str, str]:
    if data.empty:
        return actual_sku, ""

    max_conn_pct = _safe_quantile(data["ConnUtilizationPct"], 0.95) if "ConnUtilizationPct" in data.columns else 0.0
    current_idx = tier_index(actual_sku, ordered_tiers)
    if current_idx == -1:
        return actual_sku, ""

    if max_conn_pct < 35:
        return ordered_tiers[max(0, current_idx - 1)], "Connections underutilized"
    if max_conn_pct > 80:
        return ordered_tiers[min(len(ordered_tiers) - 1, current_idx + 1)], "Connections intensive"
    return actual_sku, ""


def component_comment(cpu_idx: int, mem_idx: int, conn_idx: int, current_idx: int) -> str:
    components = {"CPU": cpu_idx, "Memory": mem_idx, "Connections": conn_idx}
    max_val = max(components.values())

    intensive, underutilized, optimal = [], [], []
    for name, val in components.items():
        if val == current_idx:
            optimal.append(name)
        elif val < current_idx and val < max_val:
            underutilized.append(name)
        elif val == max_val:
            intensive.append(name)

    parts = []
    if intensive:
        parts.append(", ".join(intensive) + " Intensive")
    if underutilized:
        parts.append(", ".join(underutilized) + " Underutilized")
    if optimal:
        parts.append(", ".join(optimal) + " Optimal Usage")
    return " ; ".join(parts)


# ===========================================================================
# 7. SQL BUILDERS (NEW 5-MIN TABLE)
# ===========================================================================


def build_cluster_inventory_query(start_date: str, end_date: str) -> str:
    return f"""
        SELECT DISTINCT
            metrics.ClusterKey,
            metrics.ClusterName,
            metrics.InstanceSize,
            metrics.ProviderName,
            metrics.RegionName,
            metrics.OrgKey,
            CAST(metrics.OrgKey AS NVARCHAR(255)) AS OrgName,
            metrics.ProjectKey
        FROM [Metrics].[MongoDBRightsizingAggregated5Min] metrics
        WHERE metrics._date BETWEEN '{start_date}' AND '{end_date}'
          AND InstanceSize IS NOT NULL
    """


def build_cluster_metrics_query(cluster_key: int, start_date: str, end_date: str, day_type: str, business_hour: str, business_hour1: str) -> str:
    return f"""
        SELECT
            ClusterKey,
            ClusterName,
            InstanceSize,
            ProviderName,
            RegionName,
            _date AS [Date],
            _hour AS [Hour],
            [type] AS [DayType],
            CASE WHEN [type] = 'Weekend' THEN 'Weekend' ELSE businessHour END AS HourType,

            MAX(CpuAvg)      AS CpuAvg,
            MAX(CpuAvgP95)   AS CpuAvgP95,
            MAX(CpuMax)      AS CpuMax,
            MAX(CpuMaxP95)   AS CpuMaxP95,
            MAX(CpuMaxGt50)  AS CpuMaxGt50,
            MAX(CpuMaxGt25)  AS CpuMaxGt25,
            MAX(CpuMaxGt10)  AS CpuMaxGt10,

            MAX(MemResidentMax)     AS MemResidentMax,
            MAX(MemResidentAvg)     AS MemResidentAvg,
            MAX(MemResidentMaxPct)  AS MemResidentMaxPct,
            MAX(MemResidentAvgPct)  AS MemResidentAvgPct,
            MAX(MemResidentP95Pct)  AS MemResidentP95Pct,

            MAX(NetInAvg)      AS NetInAvg,
            MAX(NetInMax)      AS NetInMax,
            MAX(NetOutAvg)     AS NetOutAvg,
            MAX(NetOutMax)     AS NetOutMax,
            MAX(NetRequestsMax) AS NetRequestsMax,

            SUM(ConnectionsMax) AS ConnectionsMax,
            SUM(ConnectionsAvg) AS ConnectionsAvg,
            MAX(ConnUtilizationPct) AS ConnUtilizationPct,

            MAX(OpcQueryMax)    AS OpcQueryMax,
            MAX(OpcInsertMax)   AS OpcInsertMax
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
# 8. MAIN PER-CLUSTER PIPELINE
# ===========================================================================


def process_cluster(cluster_key: int, cluster_name: str, instance_size: str, provider_name: str, region_name: str, config: dict, metacache: dict) -> dict | None:
    start_date = config["StartDate"]
    end_date = config["EndDate"]
    day_type = config["Type"]
    bh = config["BusinessHour"]
    bh1 = config["BusinessHour1"]

    meta_key = (str(provider_name).upper(), str(region_name).upper())
    if meta_key not in metacache:
        print(f"[DEBUG] Loading metaconfig for provider={meta_key[0]}, region={meta_key[1]}")
        metacache[meta_key] = load_metaconfig(meta_key[0], meta_key[1])
    specs, ordered_tiers = metacache[meta_key]

    if not ordered_tiers:
        print(f"[DEBUG] No ordered_tiers found for provider={provider_name}, region={region_name}. Skipping cluster {cluster_name}.")
        return None

    actual_sku = str(instance_size).upper()
    if actual_sku not in ordered_tiers:
        print(f"[DEBUG] Actual SKU {actual_sku} not in ordered_tiers for cluster {cluster_name}. Skipping.")
        return None

    sql = build_cluster_metrics_query(cluster_key, start_date, end_date, day_type, bh, bh1)
    data = fetch_data(sql)
    if data.empty:
        print(f"[DEBUG] No metrics data for cluster {cluster_name} (key={cluster_key}) in date range {start_date} to {end_date}.")
        return None

    data["InstanceSize"] = actual_sku

    cpu_rec = recommendations("Cpu", data, actual_sku, ordered_tiers)
    mem_rec = recommendations("Mem", data, actual_sku, ordered_tiers)
    conn_sku, conn_comment = connections_recommendation(data, actual_sku, ordered_tiers, specs)

    cpu_sku = cpu_rec[0][0] if cpu_rec else actual_sku
    mem_sku = mem_rec[0][0] if mem_rec else actual_sku

    current_idx = tier_index(actual_sku, ordered_tiers)
    cpu_idx = tier_index(cpu_sku, ordered_tiers)
    mem_idx = tier_index(mem_sku, ordered_tiers)
    conn_idx = tier_index(conn_sku, ordered_tiers)

    overall_idx = max(cpu_idx, mem_idx, conn_idx)
    overall_sku = ordered_tiers[overall_idx]

    current_cost = specs.get(actual_sku, {}).get("CostPrHour", 0.0)
    recommended_cost = specs.get(overall_sku, {}).get("CostPrHour", 0.0)
    hours_in_month = _hours_in_range(start_date, end_date)
    spend_30_days = current_cost * hours_in_month
    estimated_monthly_savings = (current_cost - recommended_cost) * hours_in_month

    comment = component_comment(cpu_idx, mem_idx, conn_idx, current_idx)
    misc_comment = f"CPU:{cpu_rec[0][2] if cpu_rec else ''} / Mem:{mem_rec[0][2] if mem_rec else ''} / Connections:{conn_comment}"

    hour_type = "Weekend" if day_type == "Weekend" else bh
    avg_cpu_max = _safe_mean(data["CpuMax"]) if "CpuMax" in data.columns else 0.0
    peak_cpu_max = _safe_max(data["CpuMax"]) if "CpuMax" in data.columns else 0.0
    mem_utilization_pct = _safe_quantile(data["MemResidentMaxPct"], 0.95) if "MemResidentMaxPct" in data.columns else 0.0
    conn_utilization_pct = _safe_quantile(data["ConnUtilizationPct"], 0.95) if "ConnUtilizationPct" in data.columns else 0.0
    action = _action_label(actual_sku, overall_sku, ordered_tiers)
    audit_utc = datetime.now(timezone.utc).replace(tzinfo=None)

    print(f"[DEBUG] Recommendation for cluster {cluster_name}: overall={overall_sku}, cpu={cpu_sku}, mem={mem_sku}, conn={conn_sku}")

    return {
        "ClusterKey": cluster_key,
        "ClusterName": cluster_name,
        "DayType": day_type,
        "HourType": hour_type,
        "CurrentSku": actual_sku,
        "CurrentCostPrHour": current_cost,
        "CpuRec": cpu_sku,
        "MemRec": mem_sku,
        "ConnRec": conn_sku,
        "AvgCpuMax": avg_cpu_max,
        "PeakCpuMax": peak_cpu_max,
        "MemUtilizationPct": mem_utilization_pct,
        "ConnUtilizationPct": conn_utilization_pct,
        "RecommendedSku": overall_sku,
        "RecommendedCostPrHour": recommended_cost,
        "EstimatedMonthlySavings": estimated_monthly_savings,
        "Comment": comment,
        "MiscComment": misc_comment,
        "CurrentEfficiency": None,
        "WithinEfficiency": None,
        "OutsideEfficiency": None,
        "Spend30days": spend_30_days,
        "WithinFamilySavings": estimated_monthly_savings,
        "OverallDifferentVersionSavings": 0.0,
        "Action": action,
        "AuditUtc": audit_utc,
    }


# ===========================================================================
# 9. OUTPUT INSERT
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
        OutsideEfficiency, Spend30days, WithinFamilySavings,
        OverallDifferentVersionSavings, Action, AuditUtc
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_COLUMNS = [
    "Month", "ClusterKey", "ClusterName", "OrgName", "ProjectKey",
    "ProviderName", "RegionName", "DayType", "HourType",
    "CurrentSku", "CurrentCostPrHour",
    "CpuRec", "MemRec", "ConnRec", "AvgCpuMax", "PeakCpuMax",
    "MemUtilizationPct", "ConnUtilizationPct", "RecommendedSku",
    "RecommendedCostPrHour", "EstimatedMonthlySavings", "Comment",
    "MiscComment", "CurrentEfficiency", "WithinEfficiency",
    "OutsideEfficiency", "Spend30days", "WithinFamilySavings",
    "OverallDifferentVersionSavings", "Action", "AuditUtc",
]


def upsert_in_chunks(df: pd.DataFrame, chunk_size: int = 100):
    total = len(df)
    print(f"[DEBUG] Starting upsert of {total} rows...")

    for start in range(0, total, chunk_size):
        chunk = df.iloc[start: start + chunk_size].copy()
        conn  = connect_to_db()
        cur   = conn.cursor()

        for _, row in chunk.iterrows():
            cur.execute("""
                MERGE [Metrics].[MongoDBRightsizingRecommendations] AS target
                USING (
                    SELECT ? AS Month,
                           ? AS ClusterKey,
                           ? AS DayType,
                           ? AS HourType
                ) AS source
                ON  target.Month      = source.Month
                AND target.ClusterKey = source.ClusterKey
                AND target.DayType    = source.DayType
                AND target.HourType   = source.HourType

                WHEN MATCHED THEN UPDATE SET
                    ClusterName                    = ?,
                    OrgName                        = ?,
                    ProjectKey                     = ?,
                    ProviderName                   = ?,
                    RegionName                     = ?,
                    CurrentSku                     = ?,
                    CurrentCostPrHour              = ?,
                    CpuRec                         = ?,
                    MemRec                         = ?,
                    ConnRec                        = ?,
                    AvgCpuMax                      = ?,
                    PeakCpuMax                     = ?,
                    MemUtilizationPct              = ?,
                    ConnUtilizationPct             = ?,
                    RecommendedSku                 = ?,
                    RecommendedCostPrHour          = ?,
                    EstimatedMonthlySavings        = ?,
                    Comment                        = ?,
                    MiscComment                    = ?,
                    CurrentEfficiency              = ?,
                    WithinEfficiency               = ?,
                    OutsideEfficiency              = ?,
                    Spend30days                    = ?,
                    WithinFamilySavings            = ?,
                    OverallDifferentVersionSavings = ?,
                    Action                         = ?,
                    AuditUtc                       = ?

                WHEN NOT MATCHED THEN INSERT (
                    Month, ClusterKey, ClusterName, OrgName, ProjectKey,
                    ProviderName, RegionName, DayType, HourType,
                    CurrentSku, CurrentCostPrHour,
                    CpuRec, MemRec, ConnRec, AvgCpuMax, PeakCpuMax,
                    MemUtilizationPct, ConnUtilizationPct,
                    RecommendedSku, RecommendedCostPrHour,
                    EstimatedMonthlySavings, Comment, MiscComment,
                    CurrentEfficiency, WithinEfficiency, OutsideEfficiency,
                    Spend30days, WithinFamilySavings,
                    OverallDifferentVersionSavings, Action, AuditUtc
                ) VALUES (
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                );
            """,
            # MERGE match keys (4)
            row["Month"], row["ClusterKey"], row["DayType"], row["HourType"],
            # UPDATE SET values (27)
            row["ClusterName"],   row["OrgName"],       row["ProjectKey"],
            row["ProviderName"],  row["RegionName"],
            row["CurrentSku"],    row["CurrentCostPrHour"],
            row["CpuRec"],        row["MemRec"],         row["ConnRec"],
            row["AvgCpuMax"],     row["PeakCpuMax"],
            row["MemUtilizationPct"], row["ConnUtilizationPct"],
            row["RecommendedSku"], row["RecommendedCostPrHour"],
            row["EstimatedMonthlySavings"],
            row["Comment"],       row["MiscComment"],
            row["CurrentEfficiency"], row["WithinEfficiency"], row["OutsideEfficiency"],
            row["Spend30days"],   row["WithinFamilySavings"],
            row["OverallDifferentVersionSavings"],
            row["Action"],        row["AuditUtc"],
            # INSERT values (31)
            row["Month"],         row["ClusterKey"],     row["ClusterName"],
            row["OrgName"],       row["ProjectKey"],
            row["ProviderName"],  row["RegionName"],
            row["DayType"],       row["HourType"],
            row["CurrentSku"],    row["CurrentCostPrHour"],
            row["CpuRec"],        row["MemRec"],         row["ConnRec"],
            row["AvgCpuMax"],     row["PeakCpuMax"],
            row["MemUtilizationPct"], row["ConnUtilizationPct"],
            row["RecommendedSku"], row["RecommendedCostPrHour"],
            row["EstimatedMonthlySavings"],
            row["Comment"],       row["MiscComment"],
            row["CurrentEfficiency"], row["WithinEfficiency"], row["OutsideEfficiency"],
            row["Spend30days"],   row["WithinFamilySavings"],
            row["OverallDifferentVersionSavings"],
            row["Action"],        row["AuditUtc"],
            )

        conn.commit()
        cur.close()
        conn.close()
        print(f"[DEBUG] Upserted {min(start + chunk_size, total)}/{total} rows...")

    print(f"[DEBUG] Upsert complete.")


# ===========================================================================
# 10. MAIN
# ===========================================================================

if __name__ == "__main__":
    today = date.today()

    # Auto-detect date range from aggregated table
    date_sql = """
        SELECT
            MIN(_date) AS MinDate,
            MAX(_date) AS MaxDate
        FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    """
    date_df = fetch_data(date_sql)
    start_dt = pd.to_datetime(date_df["MinDate"].iloc[0])
    end_dt   = pd.to_datetime(date_df["MaxDate"].iloc[0])

    months      = [end_dt.strftime("%Y-%m")]
    start_dates = [start_dt.strftime("%Y-%m-%d")]
    end_dates   = [end_dt.strftime("%Y-%m-%d")]

    print(f"[DEBUG] Date range: {start_dates[0]} to {end_dates[0]} | Month: {months[0]}")

    types = ["Weekday", "Weekday", "Weekend"]
    business_hours = ["BusinessHours", "NonBusinessHours", "BusinessHours"]
    business_hours1 = ["BusinessHours", "NonBusinessHours", "NonBusinessHours"]

    month_dict = {}
    for month, start, end in zip(months, start_dates, end_dates):
        month_dict[month] = [
            {"Type": t, "BusinessHour": bh, "BusinessHour1": bh1, "StartDate": start, "EndDate": end}
            for t, bh, bh1 in zip(types, business_hours, business_hours1)
        ]

    columns = INSERT_COLUMNS.copy()

    out_df = pd.DataFrame(columns=columns)
    metacache = {}


    for month, configs in month_dict.items():
        print(f"[DEBUG] Processing month: {month}")
        for config in configs:
            print(f"[DEBUG] Config: {config}")
            _count(config["StartDate"], config["EndDate"])

            inv_sql = build_cluster_inventory_query(config["StartDate"], config["EndDate"])
            clusters = fetch_data(inv_sql)
            print(f"[DEBUG] Cluster inventory returned {clusters.shape[0]} rows.")
            if clusters.empty:
                print(f"[DEBUG] No clusters found for date range {config['StartDate']} to {config['EndDate']}.")
                continue

            for _, row in clusters.iterrows():
                cluster_key = int(row["ClusterKey"])
                cluster_name = str(row["ClusterName"])
                instance_size = str(row["InstanceSize"])
                provider_name = str(row.get("ProviderName", ""))
                region_name = str(row.get("RegionName", ""))
                org_key = row.get("OrgKey", "")
                project_key = row.get("ProjectKey", "")

                print(f"[DEBUG] Processing cluster: {cluster_name} (key={cluster_key})")
                result = process_cluster(
                    cluster_key,
                    cluster_name,
                    instance_size,
                    provider_name,
                    region_name,
                    config,
                    metacache,
                )
                if result is None:
                    print(f"[DEBUG] No recommendation generated for cluster {cluster_name} (key={cluster_key}).")
                    continue

                result["Month"] = month
                result["OrgName"] = row.get("OrgName") or org_key
                result["ProjectKey"] = project_key
                result["ProviderName"] = provider_name
                result["RegionName"] = region_name

                out_df = pd.concat([out_df, pd.DataFrame([result])], ignore_index=True)

    if not out_df.empty:
        print(f"Total recommendations: {len(out_df)}")
        print(out_df[["ClusterName", "CurrentSku", "RecommendedSku", "Comment"]].to_string())
        upsert_in_chunks(out_df)
        print("Results upserted into [Metrics].[MongoDBRightsizingRecommendations].")
    else:
        print("No recommendations generated.")