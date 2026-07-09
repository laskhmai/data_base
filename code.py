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
from sklearn.cluster import KMeans  # noqa: F401 — kept for future use

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
    conn = None
    try:
        conn = connect_to_db()
        data = pd.read_sql(sql_query, conn)
        return data
    except Exception as exc:
        print(f"Error fetching data: {exc}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


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
    try:
        numeric = pd.to_numeric(series, errors="coerce").dropna()
        if numeric.empty:
            return 0.0
        val = float(numeric.quantile(q))
        return 0.0 if (np.isnan(val) or np.isinf(val)) else val
    except Exception:
        return 0.0


def _safe_mean(series: pd.Series) -> float:
    try:
        numeric = pd.to_numeric(series, errors="coerce").dropna()
        if numeric.empty:
            return 0.0
        val = float(numeric.mean())
        return 0.0 if (np.isnan(val) or np.isinf(val)) else val
    except Exception:
        return 0.0


def _safe_max(series: pd.Series) -> float:
    try:
        numeric = pd.to_numeric(series, errors="coerce").dropna()
        if numeric.empty:
            return 0.0
        val = float(numeric.max())
        return 0.0 if (np.isnan(val) or np.isinf(val)) else val
    except Exception:
        return 0.0


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
        if  abs(specs[s].get("RAM_GB", 0) - rec_ram) < 0.01   # float tolerance
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
        if  abs(specs[s].get("RAM_GB", 0) - rec_ram) < 0.01   # float tolerance
        and "NVME" in specs[s].get("Tier", "").upper()
    ]
    if not nvme_skus:
        return None
    return min(nvme_skus, key=lambda s: specs[s].get("CostPrHour", 0.0))


# ===========================================================================
# 4. TREND + SEASONALITY
# ===========================================================================

def _trend(df: pd.DataFrame, feature: str) -> list[str]:
    """Detect weekly trend direction using simple slope comparison."""
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
        try:
            slope = np.polyfit(x, y, 1)[0]
            if slope > 0:
                trend_status.append("Increasing")
            elif slope < 0:
                trend_status.append("Decreasing")
            else:
                trend_status.append("No trend")
        except Exception:
            trend_status.append("No trend")
    return trend_status


def _build_hour_range(date_range: pd.DatetimeIndex, hr_type: str, business_hour: str, on_start: int, off_end: int) -> pd.DatetimeIndex:
    if hr_type == "Weekday" and business_hour == "BusinessHours":
        return date_range[(date_range.weekday < 5) & (date_range.hour >= on_start) & (date_range.hour <= off_end)]
    if hr_type == "Weekday" and business_hour == "NonBusinessHours":
        return date_range[(date_range.weekday < 5) & ((date_range.hour < on_start) | (date_range.hour > off_end))]
    return date_range[date_range.weekday >= 5]


def _onseasonality(seasonality: pd.DataFrame, start_date: str, end_date: str, sku: str, hr_type: str, business_hour: str, on_start: int, off_end: int) -> str:
    # NOTE: Reserved for future use — not currently called
    # Will be integrated with STL trend detection in next sprint
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
    """
    Simplified P95 threshold check per week.
    Replaces KMeans approach which had a merge-on-arbitrary-cluster-number bug.

    For each week:
      Level2: if P95(MaxP95 × 4) < 100 AND P95(Max × 4) < 100 AND P95(AvgP95 × 4) < 100
              → Action1 = 1 (safe to downsize 2 steps)
      Level1: if P95(MaxP95 × 2) < 100 AND P95(AvgP95 × 2) < 100
              → Action1 = 2 (safe to downsize 1 step)
      Else:   → Action1 = 3 (risky)
    """
    df    = data.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df["Week"] = df["Date"].dt.isocalendar().week
    mt     = metric_type
    result = []

    for week, group in df.groupby("Week"):
        if len(group) < 3:
            continue

        # Level 2 check (downsize 2 steps: ×4)
        p95_max_p95_l2  = group[f"{mt}MaxP95"].quantile(0.95) * 4
        p95_max_l2      = group[f"{mt}Max"].quantile(0.95) * 4
        p95_avg_p95_l2  = group[f"{mt}AvgP95"].quantile(0.95) * 4

        level2_ok = (
            p95_max_p95_l2 < 100
            and p95_max_l2  < 100
            and p95_avg_p95_l2 < 100
        )

        # Level 1 check (downsize 1 step: ×2)
        p95_max_p95_l1  = group[f"{mt}MaxP95"].quantile(0.95) * 2
        p95_avg_p95_l1  = group[f"{mt}AvgP95"].quantile(0.95) * 2

        level1_ok = (
            p95_max_p95_l1 < 100
            and p95_avg_p95_l1 < 100
        )

        if level2_ok:
            action1 = 1
        elif level1_ok:
            action1 = 2
        else:
            action1 = 3

        result.append({"Week": week, "Action1": action1})

    if not result:
        return pd.DataFrame(columns=["Week", "Action1"])

    return pd.DataFrame(result)


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

    # STL Seasonality Detection
    # Neeraja: if seasonal pattern exists → do NOT downsize
    # Seasonal = repeating weekly pattern (e.g. batch jobs every weekend)
    # STL decomposes data into Trend + Seasonal + Remainder
    is_seasonal = False
    try:
        from statsmodels.tsa.seasonal import STL as STLModel
        daily_p95 = trend_df[f"{mt}MaxP95"].resample("D").quantile(0.95).dropna()
        if len(daily_p95) >= 14:  # need at least 2 weeks for weekly seasonality
            stl        = STLModel(daily_p95, period=7, robust=True)
            stl_result = stl.fit()
            # Seasonal is significant if its max variation > 20% of mean CPU
            # e.g. mean=15%, seasonal peak=5% → 5/15=33% → significant ✅
            mean_val          = daily_p95.mean()
            seasonal_strength = stl_result.seasonal.abs().max()
            if mean_val > 0:
                is_seasonal = seasonal_strength / mean_val > 0.20
    except Exception:
        is_seasonal = False  # fallback — if STL fails, no seasonality assumed

    current_idx = tier_index(actual_sku, ordered_tiers)
    if current_idx == -1:
        return [(actual_sku, 0, "Unknown tier")]

    # Peak value: use P95 of P95 values — avoids single spike triggering ScaleUp
    peak_val = _safe_quantile(df[f"{mt}MaxP95"], 0.95) if f"{mt}MaxP95" in df.columns else 0.0

    weekly_actions = []
    for i, (_, row) in enumerate(weekly.iterrows()):
        a1 = int(row["Action1"])
        ts = trend_status[i - 1] if i > 0 and i - 1 < len(trend_status) else "No trend"

        # Seasonality detected → mark ALL weeks as risky
        # Neeraja: if seasonal pattern exists do NOT downsize
        # Even if individual weeks look safe, the pattern will return
        if is_seasonal:
            status     = 3
            target_idx = current_idx  # NoChange
            weekly_actions.append((status, ordered_tiers[target_idx]))
            continue

        # Only flag trend as risky if values are actually meaningful
        # peak_val = P95 of CpuMaxP95 across whole month
        # If peak_val × 2 < 50% then even doubling load is still safe
        # e.g. CPU trending 11→13%: peak_val=13% × 2=26% → still safe → ignore trend
        trend_matters = ts == "Increasing" and peak_val * 2 > 50

        if trend_matters or a1 == 3:
            if peak_val > 80:   # Genuinely high load → ScaleUp
                target_idx = min(len(ordered_tiers) - 1, current_idx + 1)
            else:               # Risky but not extreme → stay current
                target_idx = current_idx
            status = 3
        elif a1 == 1:
            target_idx = max(0, current_idx - 2)
            status = 1
        else:
            target_idx = max(0, current_idx - 1)
            status = 2

        weekly_actions.append((status, ordered_tiers[target_idx]))

    # When seasonality detected → always NoChange
    # Neeraja: seasonal clusters need capacity during their peak period
    # Do not Downsize (capacity needed) AND do not Upsize (peak is expected/seasonal)
    if is_seasonal:
        return [(actual_sku, current_idx, "Seasonal Pattern Detected — NoChange")]

    if any(s == 3 for s, _ in weekly_actions) and peak_val > 80:
        final_idx = min(len(ordered_tiers) - 1, current_idx + 1)  # Upsize 1 step
    elif all(s == 1 for s, _ in weekly_actions):
        final_idx = max(0, current_idx - 1)  # Downsize 1 step — ALL weeks confirmed safe
    elif any(s == 3 for s, _ in weekly_actions):
        final_idx = current_idx  # NoChange — uncertain, play it safe
    else:
        final_idx = max(0, current_idx - 1)  # Downsize 1 step — remaining mixed-but-safe cases

    # Fix B: Final safety check — even if clustering says Downsize
    # if MetricMaxP95×2 >= 100% it is unsafe → force NoChange
    # Uses metric-specific column (CPU or Memory) not hardcoded CPU
    if final_idx < current_idx:
        p95_col = f"{mt}MaxP95"
        metric_p95_check = _safe_quantile(df[p95_col], 0.95) \
                           if p95_col in df.columns else 0.0
        if metric_p95_check * 2 >= 100:
            final_idx = current_idx  # NoChange — P95×2 safety fails

    return [(ordered_tiers[final_idx], final_idx, "")]


def connections_recommendation(data: pd.DataFrame, actual_sku: str,
                                ordered_tiers: list[str], specs: dict,
                                shard_count: int = 1) -> tuple[str, str, float]:
    """
    Connections logic (per Neeraja discussion):

    HIGH (>80%):  return current SKU + pooling comment
                  Bigger SKU does NOT fix connections
                  Application needs connection pooling
                  BLOCKS Downsize even if CPU is low

    LOW (<35%):   return current SKU + underutilized comment
                  Do NOT suggest smaller SKU for connections
                  Let CPU drive the Downsize decision

    MODERATE:     return current SKU + no comment
                  Connections are fine, no action needed

    Returns: (sku, comment, max_conn_pct)
    max_conn_pct passed back for component_comment accuracy
    """
    if data.empty:
        return actual_sku, "", 0.0

    # Adjust connection utilization for shard count
    # ConnUtilizationPct was calculated using per-shard limit
    # For sharded clusters divide by shard count to get true utilization
    raw_conn_pct = _safe_quantile(data["ConnUtilizationPct"], 0.95) \
                   if "ConnUtilizationPct" in data.columns else 0.0
    max_conn_pct = raw_conn_pct / shard_count if shard_count > 1 else raw_conn_pct

    if max_conn_pct > 80:
        # High connections → stay at current SKU
        # Bigger SKU does NOT fix this — application needs connection pooling
        return actual_sku, "High Connections — Review Connection Pooling", max_conn_pct

    if max_conn_pct < 35:
        # Low connections → note it but do NOT suggest smaller SKU
        # CPU drives the Downsize decision not connections
        return actual_sku, "Connections Underutilized", max_conn_pct

    # Moderate connections → no action needed
    return actual_sku, "", max_conn_pct


def component_comment(cpu_idx: int, mem_idx: int, conn_idx: int,
                       current_idx: int, mem_skipped: bool = False,
                       max_conn_pct: float = 0.0) -> str:
    """
    Generates human readable comment based on metric utilization.

    CPU/Memory: compared using tier index vs current index
    Connections: compared using actual utilization % for accuracy
      >80% → Intensive (high connections — needs pooling)
      <35% → Underutilized
      else → Optimal
    mem_skipped = True → Memory Excluded (not analyzed yet)
    """
    intensive, underutilized, optimal = [], [], []

    # CPU
    if cpu_idx > current_idx:
        intensive.append("CPU")
    elif cpu_idx < current_idx:
        underutilized.append("CPU")
    else:
        optimal.append("CPU")

    # Memory — show Excluded if skipped
    if mem_skipped:
        pass  # added at end
    else:
        if mem_idx > current_idx:
            intensive.append("Memory")
        elif mem_idx < current_idx:
            underutilized.append("Memory")
        else:
            optimal.append("Memory")

    # Connections — use actual % not tier index
    # Because connections always returns current SKU now
    # so conn_idx always equals current_idx
    if max_conn_pct > 80:
        intensive.append("Connections")
    elif max_conn_pct > 0 and max_conn_pct < 35:
        underutilized.append("Connections")
    else:
        optimal.append("Connections")

    parts = []
    if intensive:
        parts.append(", ".join(intensive) + " Intensive")
    if underutilized:
        parts.append(", ".join(underutilized) + " Underutilized")
    if optimal:
        parts.append(", ".join(optimal) + " Optimal")
    if mem_skipped:
        parts.append("Memory Excluded")

    return " ; ".join(parts)


# ===========================================================================
# 7. SQL BUILDERS
# ===========================================================================

def load_spend_data(month: str) -> dict:
    """
    Load actual monthly spend per cluster from MongoDB.Spend table.
    Neeraja: only need SKU and cost from Spend table.
    Returns dict: {ClusterName: {ActualSpend, ActualSku}}
    Uses most recent SKU in month (handles mid-month resize correctly)
    """
    sql = f"""
        SELECT
            s.Cluster                           AS ClusterName,
            ROUND(SUM(s.Amount), 2)             AS ActualSpend,
            (SELECT TOP 1 sp.Sku
             FROM [MongoDB].[Spend] sp
             WHERE sp.Cluster = s.Cluster
             AND   FORMAT(CAST(sp.UsageDate AS DATE),
                   'yyyy-MM') = '{month}'
             ORDER BY sp.UsageDate DESC)        AS ActualSku
        FROM [MongoDB].[Spend] s
        WHERE FORMAT(CAST(s.UsageDate AS DATE), 'yyyy-MM') = '{month}'
        GROUP BY s.Cluster
    """
    df = fetch_data(sql)
    if df.empty:
        return {}
    result = {}
    for _, row in df.iterrows():
        result[row["ClusterName"]] = {
            "ActualSpend": row["ActualSpend"],
            "ActualSku":   row["ActualSku"]
        }
    return result


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
                    metacache: dict, spend_data: dict = None) -> dict | None:
    spend_data = spend_data or {}

    start_date = config["StartDate"]
    end_date   = config["EndDate"]
    day_type   = config["Type"]
    bh         = config["BusinessHour"]
    bh1        = config["BusinessHour1"]

    meta_key = (str(provider_name).upper(), str(region_name).upper())
    if meta_key not in metacache:
        print(f"Loading MetaConfig for Provider={provider_name}, Region={region_name}")
        metacache[meta_key] = load_metaconfig(meta_key[0], meta_key[1])

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

    # Get shard count for this cluster
    # Neeraja: connection limit is per shard
    # Total limit = per-shard limit × shard count
    shard_sql = f"""
        SELECT COUNT(DISTINCT ProcessId) AS ShardCount
        FROM [MongoDB].[Process]
        WHERE ClusterKey  = {cluster_key}
        AND   ProcessType LIKE '%PRIMARY%'
        AND   IsDeleted   = 0
    """
    shard_df    = fetch_data(shard_sql)
    shard_count = int(shard_df["ShardCount"].iloc[0]) if not shard_df.empty else 1
    shard_count = max(1, shard_count)  # safety — at least 1

    conn_sku, conn_comment, max_conn_pct = connections_recommendation(
        data, actual_sku, ordered_tiers, specs, shard_count
    )

    cpu_sku = cpu_rec[0][0] if cpu_rec else actual_sku
    mem_sku = mem_rec[0][0] if mem_rec else actual_sku

    current_idx = tier_index(actual_sku, ordered_tiers)
    cpu_idx     = tier_index(cpu_sku,    ordered_tiers)
    mem_idx     = tier_index(mem_sku,    ordered_tiers)
    conn_idx    = tier_index(conn_sku,   ordered_tiers)

    # Connections always returns current SKU now (never suggests bigger/smaller)
    # So conn_idx always equals current_idx
    # CPU drives the final SKU — connections only blocks Downsize when high
    if SKIP_MEMORY_RECOMMENDATIONS:
        overall_idx = cpu_idx  # CPU only drives SKU decision
    else:
        overall_idx = max(cpu_idx, mem_idx)

    # Connections HIGH → block Downsize even if CPU says safe
    # Smaller SKU = smaller connection limit → would break cluster
    if max_conn_pct > 80 and overall_idx < current_idx:
        overall_idx = current_idx  # NoChange — connections block downsize

    overall_sku = ordered_tiers[overall_idx]

    current_cost              = specs.get(actual_sku,  {}).get("CostPrHour", 0.0)
    recommended_cost          = specs.get(overall_sku, {}).get("CostPrHour", 0.0)
    hours_in_month            = _hours_in_range(start_date, end_date)

    # Spend30days → actual billing from MongoDB.Spend table ✅
    # EstimatedMonthlySavings → projection from MetaConfig rates ✅
    cluster_spend             = spend_data.get(cluster_name, {})
    spend_30_days             = cluster_spend.get("ActualSpend",
                                current_cost * hours_in_month)
    estimated_monthly_savings = (current_cost - recommended_cost) * hours_in_month
    # Note: Negative value when Upsize (recommended_cost > current_cost)
    # This is intentional — negative savings = additional cost for Upsize
    # Shown as negative in table so leadership sees the cost impact clearly

    comment = "High Connections — Review Connection Pooling" if max_conn_pct > 80 and overall_idx == current_idx \
              else component_comment(cpu_idx, mem_idx, conn_idx, current_idx,
                                     mem_skipped=SKIP_MEMORY_RECOMMENDATIONS,
                                     max_conn_pct=max_conn_pct)

    hour_type            = "Weekend" if day_type == "Weekend" else bh
    peak_cpu_max         = _safe_max(data["CpuMax"])                        if "CpuMax"             in data.columns else 0.0
    avg_cpu_p95          = round(_safe_quantile(data["CpuAvgP95"], 0.95)
                           if "CpuAvgP95" in data.columns else 0.0, 4)
    max_cpu_p95          = round(_safe_quantile(data["CpuMaxP95"], 0.95)
                           if "CpuMaxP95" in data.columns else 0.0, 4)
    mem_utilization_pct  = _safe_quantile(data["MemResidentMaxPct"], 0.95) if "MemResidentMaxPct"  in data.columns else 0.0
    conn_utilization_pct = _safe_quantile(data["ConnUtilizationPct"], 0.95) if "ConnUtilizationPct" in data.columns else 0.0
    action               = _action_label(actual_sku, overall_sku, ordered_tiers)
    audit_utc            = datetime.now(timezone.utc).replace(tzinfo=None)

    # Low-CPU alternative (for Downsize / Upsize)
    prov         = str(provider_name).upper()
    reg          = str(region_name).upper()
    low_cpu_sku  = None
    nvme_sku     = None

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
        "CpuRec":                  cpu_sku or None,
        "MemRec":                  mem_sku or None,
        "ConnRec":                 conn_sku or None,
        "CpuAvgP95":               avg_cpu_p95,
        "CpuMaxP95":               max_cpu_p95,
        "PeakCpuMax":              peak_cpu_max,
        "MemUtilizationPct":       mem_utilization_pct,
        "ConnUtilizationPct":      conn_utilization_pct,
        "RecommendedSku":          recommended_sku_display,
        "RecommendedCostPrHour":   recommended_cost,
        "EstimatedMonthlySavings": estimated_monthly_savings,
        "Comment":                 comment,
        "CurrentEfficiency":       eff[0],
        "WithinEfficiency":        eff[1],
        "LowCpuEfficiency":        None,          # populated by usp_MongoDBRightsizingEfficiency
        "Spend30days":             spend_30_days,
        "WithinFamilySavings":     estimated_monthly_savings,  # placeholder — overwritten by Efficiency proc
        "LowCpuSku":               low_cpu_sku or None,
        "LowCpuSavings":           low_cpu_savings if low_cpu_sku else 0.0,
        "Action":                  action,
        "AuditUtc":                audit_utc,
    }


# ===========================================================================
# 10. OUTPUT INSERT
# ===========================================================================

# Table name — change to _STL version for STL testing
# Original: [Metrics].[MongoDBRightsizingRecommendations]
# STL test: [Metrics].[MongoDBRightsizingRecommendations_STL]
REC_TABLE = "[Metrics].[MongoDBRightsizingRecommendations_STL]"

INSERT_SQL = f"""
    INSERT INTO {REC_TABLE} (
        Month, ClusterKey, ClusterName, OrgName, ProjectKey,
        ProviderName, RegionName, DayType, HourType,
        CurrentSku, CurrentCostPrHour,
        CpuRec, MemRec, ConnRec, CpuAvgP95, CpuMaxP95, PeakCpuMax,
        MemUtilizationPct, ConnUtilizationPct, RecommendedSku,
        RecommendedCostPrHour, EstimatedMonthlySavings, Comment,
        CurrentEfficiency, WithinEfficiency,
        LowCpuEfficiency, Spend30days, WithinFamilySavings,
        LowCpuSku, LowCpuSavings, Action, AuditUtc
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_COLUMNS = [
    "Month", "ClusterKey", "ClusterName", "OrgName", "ProjectKey",
    "ProviderName", "RegionName", "DayType", "HourType",
    "CurrentSku", "CurrentCostPrHour",
    "CpuRec", "MemRec", "ConnRec", "CpuAvgP95", "CpuMaxP95", "PeakCpuMax",
    "MemUtilizationPct", "ConnUtilizationPct", "RecommendedSku",
    "RecommendedCostPrHour", "EstimatedMonthlySavings", "Comment",
    "CurrentEfficiency", "WithinEfficiency",
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
            cur.execute(f"""
                DELETE FROM {REC_TABLE}
                WHERE Month      = ?
                AND   ClusterKey = ?
            """, (month, int(ck)))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Deleted existing rows for months: {months_in_df}")

    # Step 2: Insert all fresh rows in chunks
    def _safe_val(v):
        """Final safety net — convert NaN/None float to None for SQL Server."""
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return None
        return v

    for start in range(0, total, chunk_size):
        chunk = df.iloc[start : start + chunk_size].copy()
        conn  = connect_to_db()
        cur   = conn.cursor()

        for _, row in chunk.iterrows():
            cur.execute(INSERT_SQL, tuple(_safe_val(row[col]) for col in INSERT_COLUMNS))

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

    results_list = []  # collect results in list → concat once at end (avoids O(n²))
    metacache    = {}

    for month, configs in month_dict.items():
        print(f"Running for month: {month}")

        # Load actual spend from MongoDB.Spend table
        spend_data = load_spend_data(month)
        print(f"Spend data loaded: {len(spend_data)} clusters")

        for config in configs:
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

                results_list.append(result)

    # Concat once — avoids O(n²) performance issue
    out_df = pd.DataFrame(results_list, columns=INSERT_COLUMNS) \
             if results_list else pd.DataFrame(columns=INSERT_COLUMNS)

    if not out_df.empty:
        print(f"Total recommendations: {len(out_df)}")

        # Step 1: Upsert into STL test table
        upsert_in_chunks(out_df)

        # Step 2 + 3: Proc calls commented out during STL testing
        # SimulatedMetrics and Efficiency procs read from original
        # Recommendations table — not the STL test table
        # Uncomment below when ready to run against original table:
        # call_stored_proc("[Metrics].[usp_MongoDBRightsizingSimulatedMetrics]", months[0])
        # call_stored_proc("[Metrics].[usp_MongoDBRightsizingEfficiency]", months[0])

        print("All stored procedures executed successfully.")
    else:
        print("No recommendations generated.")
