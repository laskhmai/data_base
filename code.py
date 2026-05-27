"""
MongoDB Atlas Rightsizing Engine
=================================
Aggregates process-level metrics per cluster, applies KMeans clustering, detects
seasonality and trend, then issues one right-sizing recommendation per cluster.

MongoDB Atlas note
------------------
Each Atlas cluster has ONE SKU (instance size), not per-process.
All processes in a cluster share the same instance class (M10, M30, M60, …).
Therefore this pipeline:
  1. Aggregates ALL process-level metrics per cluster.
  2. Issues ONE recommendation per cluster.
"""

# ---------------------------------------------------------------------------
# Standard-library imports
# ---------------------------------------------------------------------------
import json
import re
import warnings
from datetime import date, datetime, timedelta

# ---------------------------------------------------------------------------
# Third-party imports
# ---------------------------------------------------------------------------
import numpy as np
import pandas as pd
import pyodbc
from dateutil.relativedelta import relativedelta
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from statsmodels.tsa.seasonal import STL

warnings.filterwarnings("ignore")


# ===========================================================================
# 1. DATABASE CONNECTION
# ===========================================================================

SERVER   = "hybridasa.sql.azuresynapse.net"
DATABASE = "hybridasa_dedicatedpool"
USERNAME = "hybridasawrite"
PASSWORD = "H@Sh1CoRS!"
DRIVER   = "{ODBC Driver 18 for SQL Server}"


def connect_to_db():
    """Open and return a pyodbc connection; exits on failure."""
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
        exit(1)


def fetch_data(sql_query: str) -> pd.DataFrame:
    """Execute *sql_query* and return the result as a DataFrame."""
    try:
        conn = connect_to_db()
        data = pd.read_sql(sql_query, conn)
        conn.close()
        return data
    except Exception as exc:
        print(f"Error fetching data: {exc}")
        return pd.DataFrame()


# ===========================================================================
# 2. HELPER UTILITIES
# ===========================================================================

def q95(x: pd.Series) -> float:
    """Return the 95th-percentile of *x*."""
    return x.quantile(0.95)


def _count(start_date: str, end_date: str) -> tuple[int, int]:
    """
    Count weekdays and weekend days between *start_date* and *end_date*
    (both inclusive, format: 'YYYY-MM-DD').
    Returns (weekday_count, weekend_count).
    """
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


# ===========================================================================
# 3. MONGODB ATLAS INSTANCE SIZES (Dynamic from MetaConfig)
# ===========================================================================

def load_atlas_instance_specs(provider: str, region: str) -> tuple[dict, list]:
    """
    Load Atlas instance specs from [Analytics].[MongoDBMetaConfig] for the given provider/region.
    Returns (specs_dict, ordered_tiers_list).
    """
    sql = f"""
        SELECT SkuName, vCores, MemorySizeGB, Instance, CostPrHour, Provider, Region
        FROM [Analytics].[MongoDBMetaConfig]
        WHERE Provider = '{provider}' AND Region = '{region}'
        ORDER BY MemorySizeGB, vCores
    """
    df = fetch_data(sql)
    if df.empty:
        raise RuntimeError(f"No Atlas meta config found for {provider}/{region}")
    specs = {}
    ordered = []
    for _, row in df.iterrows():
        tier = row['SkuName']
        specs[tier] = {
            'RAM_GB': row['MemorySizeGB'],
            'vCPUs': row['vCores'],
            'MaxConnections': None,  # Optionally add if available
            'CostPrHour': row['CostPrHour'],
            'Provider': row['Provider'],
            'Region': row['Region'],
        }
        ordered.append(tier)
    return specs, ordered

def get_tier_index(instance_size: str, ordered_tiers: list) -> int:
    try:
        return ordered_tiers.index(instance_size)
    except ValueError:
        return -1

def get_tier_ram_mb(instance_size: str, specs: dict) -> float:
    s = specs.get(instance_size)
    return s['RAM_GB'] * 1024 if s else 0

def get_tier_max_connections(instance_size: str, specs: dict) -> int:
    s = specs.get(instance_size)
    return s['MaxConnections'] if s and s['MaxConnections'] is not None else 0


# ===========================================================================
# 4. TREND DETECTION (STL Decomposition)
# ===========================================================================

def _Trend(x: pd.DataFrame, freq: str, feature: str) -> list[str]:
    """
    Detect weekly trend direction using STL decomposition.

    Parameters
    ----------
    x       : DataFrame indexed by datetime with at least *feature* column.
    freq    : Resample frequency string (e.g. 'W').
    feature : Column name to analyse.

    Returns
    -------
    List of 'Increasing' | 'Decreasing' | 'No trend' per consecutive week pair.
    """
    if x.empty:
        return ["Empty Downsize"]

    def _check_direction(data_resampled: pd.Series, period: int) -> tuple[str, pd.Series]:
        stl    = STL(data_resampled, period=period)
        result = stl.fit()
        if result.trend.sum() != 0:
            direction = "Increasing" if result.trend[-1] > result.trend[0] else "Decreasing"
            return direction, result.trend
        return "No trend", result.trend

    weekly_data   = x.resample(freq).quantile(0.95)
    weekly_status: list[str] = []

    for i in range(len(weekly_data) - 1):
        week_slice = weekly_data.iloc[i : i + 2]
        direction, _ = _check_direction(week_slice[feature], period=2)
        weekly_status.append(direction)

    return weekly_status


# ===========================================================================
# 5. SEASONALITY DETECTION
# ===========================================================================

def _build_hour_range(
    date_range: pd.DatetimeIndex,
    hr_type: str,
    business_hour: str,
    on_start: int,
    off_end: int,
) -> pd.DatetimeIndex:
    """Filter *date_range* to the relevant business / off-hours window."""
    if hr_type == "Weekday" and business_hour == "BusinessHours":
        return date_range[
            (date_range.weekday < 5)
            & (date_range.hour >= on_start)
            & (date_range.hour <= off_end)
        ]
    if hr_type == "Weekday" and business_hour == "NonBusinessHours":
        return date_range[
            (date_range.weekday < 5)
            & ((date_range.hour < on_start) | (date_range.hour > off_end))
        ]
    # Weekend
    return date_range[date_range.weekday >= 5]


def _onseasonality(
    seasonality: pd.DataFrame,
    start_date: str,
    end_date: str,
    sku: str,
    hr_type: str,
    business_hour: str,
    on_start: int,
    off_end: int,
) -> str:
    """
    Detect hourly / daily seasonality for *sku* within the given date window.
    Returns a descriptive string such as 'Seasonality : Hourly,Daily'.
    """
    x = (
        seasonality
        .groupby(["Date", "Hour", "Action"])
        .size()
        .reset_index(name="_count")
        .copy()
    )
    x["_datetime"] = pd.to_datetime(x["Date"]) + pd.to_timedelta(x["Hour"], unit="h")

    if x[x["Action"] == sku].empty:
        return "Empty"

    date_range  = pd.date_range(start=start_date, end=f"{end_date} 23:59:00", freq="h")
    hrs_range   = _build_hour_range(date_range, hr_type, business_hour, on_start, off_end)

    df_l1 = (
        x[x["Action"] == sku]
        .set_index("_datetime")
        .reindex(hrs_range, fill_value=0)
        .reset_index()
    )
    df_l1.columns = ["_datetime", "Date", "Hour", "Action", "_count"]
    df_l1 = df_l1[["_datetime", "_count"]].set_index("_datetime")

    hourly_p90 = df_l1.groupby(df_l1.index.hour).quantile(0.9)
    daily_p90  = df_l1.groupby(df_l1.index.date).quantile(0.9)

    hourly = int((hourly_p90["_count"] >= 1).any())
    daily  = int((daily_p90["_count"].sum() > 1))

    if hourly and daily:
        return "Seasonality : Hourly,Daily"
    if hourly:
        return "Seasonality : Hourly"
    if daily:
        return "Seasonality : Daily"
    return "No Seasonality : Daily or Hourly"


def wSeasonality(
    seasonality: pd.DataFrame,
    start_date: str,
    end_date: str,
    sku: str,
    hr_type: str,
    business_hour: str,
    on_start: int,
    off_end: int,
) -> str:
    """
    Detect day-of-week seasonality for *sku*.
    Returns 'Seasonality : Day-of-Week' or 'No Seasonality : Day-of-Week'.
    """
    x = (
        seasonality
        .groupby(["Date", "Hour", "Action"])
        .size()
        .reset_index(name="_count")
        .copy()
    )
    x["_datetime"] = pd.to_datetime(x["Date"]) + pd.to_timedelta(x["Hour"], unit="h")

    if x[x["Action"] == sku].empty:
        return "Empty"

    date_range = pd.date_range(start=start_date, end=f"{end_date} 23:59:00", freq="h")
    hrs_range  = _build_hour_range(date_range, hr_type, business_hour, on_start, off_end)

    df_l1 = (
        x[x["Action"] == sku]
        .set_index("_datetime")
        .reindex(hrs_range, fill_value=0)
        .reset_index()
    )
    df_l1.columns = ["_datetime", "Date", "Hour", "Action", "_count"]
    df_l1 = df_l1[["_datetime", "_count"]].set_index("_datetime")

    day_names   = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    percentile  = df_l1.groupby([df_l1.index.dayofweek, df_l1.index.hour]).quantile(0.9).unstack(level=0)
    percentile.columns = [day_names[i] for i in range(len(percentile.columns))]

    if not percentile[percentile == 1].dropna(how="all").empty:
        return "Seasonality : Day-of-Week"
    return "No Seasonality : Day-of-Week"


# ===========================================================================
# 6. CLUSTERING & RIGHTSIZING
# ===========================================================================

def perform_clustering_and_rightsizing(
    metric_type: str,
    data: pd.DataFrame,
    hr_type: str,
    business_hour: str,
    actual_sku: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Apply KMeans clustering (3 clusters) at two scaling levels per week,
    then return a DataFrame of weekly cluster metrics with an 'Action1' column.

    Level-2 features: metric x 4  (aggressive downsize candidate)
    Level-1 features: metric x 2  (moderate downsize candidate)

    For MongoDB:
      - Cpu uses: CpuMaxP95, CpuMax, CpuAvgP95
      - Mem uses: MemMaxP95, MemMax, MemAvgP95  (as % of tier RAM)
    """
    df = data.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df["Week"] = df["Date"].dt.isocalendar().week

    weekly_cluster_metrics: list[pd.DataFrame] = []
    mt = metric_type

    for week, group in df.groupby("Week"):
        if len(group) < 3:
            # Not enough data for 3 clusters
            continue

        # --- Level-2 (x4) ---
        level2_cols = [f"{mt}MaxP95", f"{mt}Max", f"{mt}AvgP95"]
        if not all(c in group.columns for c in level2_cols):
            continue
        level2 = group[level2_cols].mul(4)
        level2.columns = [f"{mt}MaxP952level", f"{mt}Max2level", f"{mt}AvgP952level"]
        km2 = KMeans(n_clusters=3, random_state=42)
        level2["Cluster"] = km2.fit_predict(level2)
        centers2 = level2.groupby("Cluster").quantile(0.95).reset_index()
        centers2["Week"] = week

        # --- Level-1 (x2) ---
        level1_cols = [f"{mt}MaxP95", f"{mt}AvgP95"]
        level1 = group[level1_cols].mul(2)
        level1.columns = [f"{mt}MaxP951level", f"{mt}AvgP951level"]
        km1 = KMeans(n_clusters=3, random_state=42)
        level1["Cluster"] = km1.fit_predict(level1)
        centers1 = level1.groupby("Cluster").quantile(0.95).reset_index()
        centers1["Week"] = week

        merged = pd.merge(centers1, centers2, on=["Cluster", "Week"])
        weekly_cluster_metrics.append(merged)

    if not weekly_cluster_metrics:
        return pd.DataFrame(columns=["Week", "Action1"])

    final_df = pd.concat(weekly_cluster_metrics, ignore_index=True)

    def assign_cluster(row: pd.Series) -> int:
        c2_ok = (
            row.get(f"{mt}MaxP952level", 101) < 100
            and row.get(f"{mt}Max2level",    101) < 100
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

    # Collapse to one row per week (take the maximum action level)
    max_per_week = (
        final_df.groupby("Week")
        .agg(Action1=("Action1", "max"))
        .reset_index()
    )
    # Restore approximate date from original data
    week_dates = df.groupby("Week")["Date"].min().reset_index()
    max_per_week = max_per_week.merge(week_dates, on="Week", how="left")

    return final_df  # caller uses the full frame with Action1


# ===========================================================================
# 7. CONNECTIONS RECOMMENDATIONS (replaces IOPS for MongoDB)
# ===========================================================================

def connections_recommendations(
    data: pd.DataFrame,
    actual_sku: str,
) -> tuple[str, str]:
    """
    Return recommended tier based on peak connection usage vs tier limits.
    Returns (recommended_tier, comment).
    """
    if data.empty:
        return actual_sku, ""

    max_conns_p95 = data["ConnectionsMaxP95"].max() if "ConnectionsMaxP95" in data.columns else 0
    current_idx = get_tier_index(actual_sku)
    if current_idx == -1:
        return actual_sku, ""

    # Find the smallest tier that can handle the peak connections
    for i, tier in enumerate(ATLAS_TIERS_ORDERED):
        tier_limit = get_tier_max_connections(tier)
        if tier_limit > max_conns_p95:
            if i < current_idx:
                return tier, "Connections underutilized"
            elif i > current_idx:
                return tier, "Connections intensive"
            else:
                return tier, ""

    return actual_sku, "Connections at maximum capacity"


# ===========================================================================
# 8. RECOMMENDATIONS (CPU / MEMORY)
# ===========================================================================

def recommendations(
    metric_type: str,
    data: pd.DataFrame,
    hr_type: str,
    business_hour: str,
    actual_sku: str,
    start_date: str,
    end_date: str,
    on_start: int,
    off_end: int,
) -> list | None:
    """
    Derive monthly CPU or Memory rightsizing recommendations.

    Parameters
    ----------
    metric_type   : 'Cpu' or 'Mem'
    data          : Hourly metrics DataFrame (cluster-level aggregated).
    hr_type       : 'Weekday' or 'Weekend'.
    business_hour : 'BusinessHours' or 'NonBusinessHours'.
    actual_sku    : Current Atlas instance size (e.g. 'M30').
    start_date    : Analysis window start ('YYYY-MM-DD').
    end_date      : Analysis window end ('YYYY-MM-DD').
    on_start      : Business-hours start (hour int, e.g. 7).
    off_end       : Business-hours end   (hour int, e.g. 18).

    Returns
    -------
    List of (recommended_tier, resource_value, comment) tuples.
    """
    if data.empty:
        return None

    mt = metric_type
    df = data.copy()

    # --- MongoDB thresholds: Gt50/Gt25 (not Gt88/Gt50 like PostgreSQL) ---
    gt_high = f"{mt}MaxGt50"
    gt_mid  = f"{mt}MaxGt25"

    # Ensure threshold columns exist
    if gt_high not in df.columns:
        df[gt_high] = 0
    if gt_mid not in df.columns:
        df[gt_mid] = 0

    # Action labelling
    df.loc[
        (df[f"{mt}MaxP95"] <= 25)
        & (df[f"{mt}AvgP95"] <= 25)
        & (df[gt_high] == 0)
        & (df[gt_mid] == 0)
        & (df["InstanceSize"] == actual_sku),
        "Action",
    ] = "L1"

    df.loc[
        (df[f"{mt}MaxP95"] > 50)
        & ((df[gt_high] > 1) | (df[gt_mid] >= 1))
        & (df["InstanceSize"] == actual_sku),
        "Action",
    ] = "L3"

    df.loc[df["InstanceSize"] != actual_sku, "Action"] = "L3"
    df["Action"] = df["Action"].fillna("L2")

    # --- Weekly clustering ---
    weekly_avg = perform_clustering_and_rightsizing(
        mt, data, hr_type, business_hour, actual_sku, start_date, end_date
    )

    if weekly_avg.empty:
        return [(actual_sku, 0, "Insufficient data for clustering")]

    # --- Trend ---
    trend_df = data.copy()
    trend_df["_datetime"] = pd.to_datetime(
        trend_df["Date"].astype(str) + " " + trend_df["Hour"].astype(str) + ":00:00"
    )
    trend_df.set_index("_datetime", inplace=True)
    z = trend_df[[f"{mt}MaxP95", f"{mt}AvgP95"]].copy()
    if gt_mid in trend_df.columns:
        z[gt_mid] = trend_df[gt_mid]
    trend_status = _Trend(z, "W", f"{mt}MaxP95")

    # --- Seasonality per week ---
    seasonality_df = df.copy()
    seasonality_df["_datetime"] = pd.to_datetime(
        seasonality_df["Date"].astype(str) + " " + seasonality_df["Hour"].astype(str) + ":00:00"
    )
    seasonality_df.set_index("_datetime", inplace=True)
    seasonality_df.sort_index(inplace=True)

    first_ts = seasonality_df.index.min()
    last_ts  = seasonality_df.index.max()
    weekly_intervals: list[tuple] = []
    current_start = pd.Timestamp(first_ts.date())

    while current_start <= last_ts:
        current_end = current_start + pd.DateOffset(days=(6 - current_start.weekday()))
        if current_end > last_ts:
            current_end = last_ts
        weekly_intervals.append((current_start, current_end))
        current_start = current_end + pd.DateOffset(days=1)

    week_flags: list[str] = []
    l3_statuses: list[str] = []
    comment = ""

    for week_start, week_end in weekly_intervals:
        mask      = (seasonality_df.index >= week_start) & (seasonality_df.index <= week_end)
        week_data = seasonality_df.loc[mask]
        ws        = week_start.strftime("%Y-%m-%d")
        we        = week_end.strftime("%Y-%m-%d")

        s3 = _onseasonality(week_data, ws, we, "L3", hr_type, business_hour, on_start, off_end)
        s2 = _onseasonality(week_data, ws, we, "L2", hr_type, business_hour, on_start, off_end)

        if "No Seasonality" not in s3 and "Empty" not in s3:
            week_flags.append("3")
            l3_statuses.append(s3)
        elif "No Seasonality" not in s2 and "Empty" not in s2:
            week_flags.append("2")
        else:
            week_flags.append("1")

    # Summarise seasonality comment
    if "3" in week_flags:
        if "Seasonality : Hourly,Daily" in l3_statuses:
            comment += "Seasonality : Hourly,Daily"
        elif "Seasonality : Hourly" in l3_statuses and "Seasonality : Daily" in l3_statuses:
            comment += "Seasonality : Hourly,Daily"
        elif "Seasonality : Hourly" in l3_statuses:
            comment += "Seasonality : Hourly"
        elif "Seasonality : Daily" in l3_statuses:
            comment += "Seasonality : Daily"
        else:
            comment += "No Seasonality : Hourly,Daily"

    wl3 = wSeasonality(seasonality_df, start_date, end_date, "L3", hr_type, business_hour, on_start, off_end)
    wl2 = wSeasonality(seasonality_df, start_date, end_date, "L2", hr_type, business_hour, on_start, off_end)

    weekly_seasonality = "1"
    if "No Seasonality" not in wl3 and "Empty" not in wl3:
        weekly_seasonality = "3"
        comment += ";" + wl3
    elif "No Seasonality" not in wl2 and "Empty" not in wl2:
        weekly_seasonality = "2"

    # --- Tier lookup ---
    current_idx = get_tier_index(actual_sku)
    if current_idx == -1:
        return [(actual_sku, 0, "Unknown tier")]

    # --- Weekly tier recommendations ---
    weekly_recs: list[tuple] = []
    for i, (_, row) in enumerate(weekly_avg.iterrows()):
        a1 = row["Action1"]
        wf = week_flags[i] if i < len(week_flags) else "1"

        if a1 == 3:
            idx = current_idx
        elif a1 == 2:
            idx = max(0, current_idx - 1)
        else:
            idx = max(0, current_idx - 2)

        if i == 0:
            if a1 == 3 or wf == "3":
                plan_idx, status = current_idx, 3
            elif a1 == 1 and wf == "1":
                plan_idx, status = idx, 1
            else:
                status = 2
                plan_idx = idx + 1 if a1 == 1 else idx
        else:
            ts = trend_status[i - 1] if i - 1 < len(trend_status) else ""
            if ts == "Increasing" or a1 == 3 or wf == "3":
                plan_idx, status = current_idx, 3
            elif a1 == 1 and wf == "1":
                plan_idx, status = idx, 1
            else:
                status = 2
                plan_idx = idx + 1 if a1 == 1 else idx

        plan_idx = min(max(0, plan_idx), len(ATLAS_TIERS_ORDERED) - 1)
        plan_tier = ATLAS_TIERS_ORDERED[plan_idx]
        weekly_recs.append((row.get("Week"), status, plan_tier))

    # --- Monthly roll-up ---
    if any(s == 3 for _, s, _ in weekly_recs) or weekly_seasonality == "3":
        final_idx = current_idx
    elif all(s == 1 for _, s, _ in weekly_recs) and weekly_seasonality == "1":
        final_idx = max(0, current_idx - 2)
    else:
        final_idx = max(0, current_idx - 1)

    final_tier = ATLAS_TIERS_ORDERED[final_idx]
    specs = ATLAS_INSTANCE_SPECS[final_tier]

    if mt == "Cpu":
        return [(final_tier, specs["vCPUs"], comment)]
    return [(final_tier, specs["RAM_GB"], comment)]


# ===========================================================================
# 9. METRIC NORMALISATION (MongoDB-specific)
# ===========================================================================

def normalize_mongodb_metrics(
    row: pd.Series,
    current_tier: str,
    recommended_tier: str,
) -> pd.Series:
    """
    Scale raw CPU / Memory metrics to equivalent percentages for a candidate tier.
    MongoDB memory is in MB; we normalise against tier RAM.
    CPU is already a normalised percentage.
    """
    result: dict = {}

    cs_specs = ATLAS_INSTANCE_SPECS.get(current_tier, {})
    rec_specs = ATLAS_INSTANCE_SPECS.get(recommended_tier, {})

    cs_vcpu = cs_specs.get("vCPUs", 1)
    rec_vcpu = rec_specs.get("vCPUs", 1)
    cs_ram_mb = cs_specs.get("RAM_GB", 1) * 1024
    rec_ram_mb = rec_specs.get("RAM_GB", 1) * 1024

    # CPU scales linearly with vCPUs
    result["nCpuAvg"] = row.get("CpuAvg", 0) * cs_vcpu / rec_vcpu
    result["nCpuMax"] = row.get("CpuMax", 0) * cs_vcpu / rec_vcpu
    result["nCpuAvgP95"] = row.get("CpuAvgP95", 0) * cs_vcpu / rec_vcpu
    result["nCpuMaxP95"] = row.get("CpuMaxP95", 0) * cs_vcpu / rec_vcpu

    # Memory: convert MB usage to % of recommended tier RAM
    result["nMemAvg"] = (row.get("MemResidentAvg", 0) / rec_ram_mb) * 100
    result["nMemMax"] = (row.get("MemResidentMax", 0) / rec_ram_mb) * 100
    result["nMemAvgP95"] = (row.get("MemAvgP95", 0) / rec_ram_mb) * 100
    result["nMemMaxP95"] = (row.get("MemMaxP95", 0) / rec_ram_mb) * 100

    return pd.Series(result)


# ===========================================================================
# 10. EFFICIENCY METRICS
# ===========================================================================

def calculate_efficiency_metrics(
    data: pd.DataFrame,
    instance_size: str,
) -> pd.DataFrame:
    """
    Compute per-row CPU and Memory efficiency scores (0-1 scale).

    Scoring logic
    -------------
    - Base efficiency:  bucketed thresholds on Avg utilisation.
    - Peak penalty:     multiplier based on Max utilisation.
    - P95 variability:  penalises high Avg<->P95 spread.
    - MaxP95 stability: penalises high Max<->MaxP95 spread.
    """
    if data is None or data.empty:
        return pd.DataFrame()

    df = data.copy()
    tier_ram_mb = get_tier_ram_mb(instance_size)

    # Compute memory percentage columns
    if tier_ram_mb > 0:
        df["MemPctAvg"] = (df["MemResidentAvg"] / tier_ram_mb) * 100
        df["MemPctMax"] = (df["MemResidentMax"] / tier_ram_mb) * 100
        df["MemPctAvgP95"] = df["MemPctAvg"].expanding().quantile(0.95)
        df["MemPctMaxP95"] = df["MemPctMax"].expanding().quantile(0.95)
    else:
        df["MemPctAvg"] = df["MemPctMax"] = df["MemPctAvgP95"] = df["MemPctMaxP95"] = 0

    # --- CPU base efficiency ---
    df["cpu_base_efficiency"] = np.where(
        df["CpuAvg"] >= 90, 0.05,
        np.where(df["CpuAvg"] >= 80, 0.15,
        np.where(df["CpuAvg"] <  15, 0.10,
        np.where(df["CpuAvg"] <  40, 0.40,
        np.where(df["CpuAvg"] <= 70, 0.75,
        np.where(df["CpuAvg"] <= 85, 0.60,
        0.35))))))

    df["cpu_peak_penalty"] = np.where(
        df["CpuMax"] >= 95, 0.4,
        np.where(df["CpuMax"] >= 90, 0.6,
        np.where(df["CpuMax"] >= 80, 0.8, 1.0)))

    df["cpu_p95_factor"] = np.where(
        df["CpuAvgP95"] > 0,
        np.where(
            abs(df["CpuAvg"] - df["CpuAvgP95"]) / np.maximum(df["CpuAvgP95"], 1) > 0.4,  0.7,
            np.where(
                abs(df["CpuAvg"] - df["CpuAvgP95"]) / np.maximum(df["CpuAvgP95"], 1) > 0.25, 0.85,
                1.0)),
        1.0)

    df["cpu_maxp95_factor"] = np.where(
        (df["CpuMax"] > 0) & (df["CpuMaxP95"] > 0),
        np.where(
            abs(df["CpuMax"] - df["CpuMaxP95"]) / np.maximum(df["CpuMaxP95"], 1) > 0.3, 0.8,
            1.0),
        1.0)

    df["cpu_efficiency_score"] = (
        df["cpu_base_efficiency"]
        * df["cpu_peak_penalty"]
        * df["cpu_p95_factor"]
        * df["cpu_maxp95_factor"]
    )

    # --- Memory base efficiency (using % of tier RAM) ---
    df["memory_base_efficiency"] = np.where(
        df["MemPctAvg"] >= 90, 0.05,
        np.where(df["MemPctAvg"] >= 80, 0.10,
        np.where(df["MemPctAvg"] <  20, 0.15,
        np.where(df["MemPctAvg"] <  40, 0.35,
        np.where(df["MemPctAvg"] <= 80, 0.70,
        0.50)))))

    df["memory_peak_penalty"] = np.where(
        df["MemPctMax"] >= 95, 0.4,
        np.where(df["MemPctMax"] >= 90, 0.6,
        np.where(df["MemPctMax"] >= 80, 0.8, 1.0)))

    df["memory_p95_factor"] = np.where(
        df["MemPctAvgP95"] > 0,
        np.where(
            abs(df["MemPctAvg"] - df["MemPctAvgP95"]) / np.maximum(df["MemPctAvgP95"], 1) > 0.4, 0.7,
            np.where(
                abs(df["MemPctAvg"] - df["MemPctAvgP95"]) / np.maximum(df["MemPctAvgP95"], 1) > 0.25, 0.85,
                1.0)),
        1.0)

    df["memory_maxp95_factor"] = np.where(
        (df["MemPctMax"] > 0) & (df["MemPctMaxP95"] > 0),
        np.where(
            abs(df["MemPctMax"] - df["MemPctMaxP95"]) / np.maximum(df["MemPctMaxP95"], 1) > 0.3, 0.8,
            1.0),
        1.0)

    df["memory_efficiency_score"] = (
        df["memory_base_efficiency"]
        * df["memory_peak_penalty"]
        * df["memory_p95_factor"]
        * df["memory_maxp95_factor"]
    )

    return df


# ===========================================================================
# 11. COMMENT UTILITIES
# ===========================================================================

def misc_comment(misc_comm: str) -> str | None:
    """
    Parse and de-duplicate seasonality comments from the combined metric string.
    Returns None if the string carries no useful information.
    """
    if misc_comm == "CPU: / Mem: / Connections:":
        return None

    misc_comm = misc_comm.replace("Seaonality", "Seasonality")

    cpu_s  = re.findall(r"CPU:Seasonality\s*:\s*(.*?)(?:/|$)",  misc_comm)
    mem_s  = re.findall(r"Mem:Seasonality\s*:\s*(.*?)(?:/|$)",  misc_comm)
    conn_s = re.findall(r"Connections:Seasonality\s*:\s*(.*?)(?:/|$)", misc_comm)

    all_patterns = ".".join(cpu_s + mem_s + conn_s)
    pattern_list = [p.strip() for part in all_patterns.split(";") for p in part.split(",") if p.strip()]
    unique_patterns = sorted(set(pattern_list))

    if not unique_patterns:
        return None

    return ", ".join(unique_patterns) + " seasonality observed in usage"


def _comment(
    cpu_idx: int,
    mem_idx: int,
    conn_idx: int,
    current_idx: int,
) -> str:
    """
    Generate a human-readable component-level utilisation comment.
    """
    if cpu_idx == mem_idx == conn_idx and current_idx != cpu_idx:
        return "CPU, Memory, Connections Underutilized"

    components = {"CPU": cpu_idx, "Memory": mem_idx, "Connections": conn_idx}
    max_val    = max(components.values())

    intensive, underutilised, optimal = [], [], []
    for name, val in components.items():
        if val == current_idx:
            optimal.append(name)
        elif val < current_idx and val < max_val:
            underutilised.append(name)
        elif val == max_val:
            intensive.append(name)

    parts = []
    if intensive:
        parts.append(", ".join(intensive) + " Intensive")
    if underutilised:
        parts.append(", ".join(underutilised) + " Underutilized")
    if optimal:
        parts.append(", ".join(optimal) + " Optimal Usage")

    return " ; ".join(parts)


# ===========================================================================
# 12. SQL QUERY BUILDER
# ===========================================================================

def build_cluster_metrics_query(
    cluster_key: int,
    start_date: str,
    end_date: str,
    day_type: str,
    business_hour: str,
    business_hour1: str,
) -> str:
    """
    Return the T-SQL string for pulling hourly MongoDB metrics aggregated
    at the cluster level (MAX across all processes in the cluster).
    """
    return f"""
        SELECT
            ClusterKey,
            ClusterName,
            InstanceSize,
            ProviderName,
            RegionName,
            _date  AS [Date],
            _hour  AS [Hour],
            [type] AS [DayType],
            CASE WHEN [type] = 'Weekend' THEN 'Weekend' ELSE businessHour END AS HourType,

            -- CPU (cluster-level = MAX across processes)
            MAX(CpuAvg)       AS CpuAvg,
            MAX(CpuMax)       AS CpuMax,
            MAX(CpuMaxGt50)   AS CpuMaxGt50,
            MAX(CpuMaxGt25)   AS CpuMaxGt25,
            MAX(CpuMaxGt10)   AS CpuMaxGt10,

            -- Memory Resident (MB - MAX across processes)
            MAX(MemResidentMax)  AS MemResidentMax,
            MAX(MemResidentAvg)  AS MemResidentAvg,

            -- Network (MAX across processes)
            MAX(NetInMax)   AS NetInMax,
            MAX(NetOutMax)  AS NetOutMax,

            -- Connections (SUM across processes - total cluster connections)
            SUM(ConnectionsMax) AS ConnectionsMax,
            SUM(ConnectionsAvg) AS ConnectionsAvg,

            -- Opcounters (MAX across processes)
            MAX(OpcQueryMax)  AS OpcQueryMax,
            MAX(OpcInsertMax) AS OpcInsertMax

        FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
        WHERE _date BETWEEN '{start_date}' AND '{end_date}'
          AND ClusterKey = {cluster_key}
          AND [type] IN ('{day_type}')
          AND (businessHour IN ('{business_hour}') OR businessHour IN ('{business_hour1}'))
        GROUP BY
            ClusterKey, ClusterName, InstanceSize, ProviderName, RegionName,
            _date, _hour, [type],
            CASE WHEN [type] = 'Weekend' THEN 'Weekend' ELSE businessHour END
        ORDER BY _date, _hour;
    """


def build_cluster_inventory_query(start_date: str, end_date: str) -> str:
    """Return SQL to fetch distinct clusters that have metrics in the date range."""
    return f"""
        SELECT DISTINCT
            m.ClusterKey,
            m.ClusterName,
            m.InstanceSize,
            m.ProviderName,
            m.RegionName,
            m.OrgKey,
            m.ProjectKey
        FROM [Metrics].[MongoDBRightsizingAggregatedHourly] m
        WHERE m._date BETWEEN '{start_date}' AND '{end_date}'
          AND m.InstanceSize IS NOT NULL
    """


# ===========================================================================
# 13. PER-CLUSTER PROCESSING
# ===========================================================================

def compute_cluster_percentiles(data: pd.DataFrame, instance_size: str) -> pd.DataFrame:
    """
    Compute P95 columns for CPU and Memory metrics at the cluster level.
    Memory is converted from MB to % of tier RAM for the clustering logic.
    """
    df = data.copy()
    tier_ram_mb = get_tier_ram_mb(instance_size)

    # CPU P95 (already in %)
    df["CpuMaxP95"] = df["CpuMax"].expanding().quantile(0.95)
    df["CpuAvgP95"] = df["CpuAvg"].expanding().quantile(0.95)

    # Memory as % of tier RAM
    if tier_ram_mb > 0:
        df["MemPctMax"] = (df["MemResidentMax"] / tier_ram_mb) * 100
        df["MemPctAvg"] = (df["MemResidentAvg"] / tier_ram_mb) * 100
    else:
        df["MemPctMax"] = 0
        df["MemPctAvg"] = 0

    df["MemMaxP95"] = df["MemPctMax"].expanding().quantile(0.95)
    df["MemAvgP95"] = df["MemPctAvg"].expanding().quantile(0.95)
    df["MemPctMaxP95"] = df["MemMaxP95"]
    df["MemPctAvgP95"] = df["MemAvgP95"]

    # For clustering: rename to match expected columns
    df["MemMax"] = df["MemPctMax"]

    # Connections P95
    df["ConnectionsMaxP95"] = df["ConnectionsMax"].expanding().quantile(0.95)

    return df


def process_cluster(
    cluster_key: int,
    cluster_name: str,
    instance_size: str,
    config: dict,
) -> dict | None:
    """
    Run the full rightsizing pipeline for one cluster + one config combination.

    Returns a dict with recommendation fields, or None if data is empty.
    """
    start_date = config["StartDate"]
    end_date   = config["EndDate"]
    day_type   = config["Type"]
    bh         = config["BusinessHour"]
    bh1        = config["BusinessHour1"]
    hr_type    = "Weekend" if day_type == "Weekend" else "Weekday"
    business_hour = bh if day_type != "Weekend" else "NonBusinessHours"

    # Fetch cluster-level hourly metrics
    sql = build_cluster_metrics_query(cluster_key, start_date, end_date, day_type, bh, bh1)
    data = fetch_data(sql)

    if data.empty:
        return None

    # Compute percentiles
    data = compute_cluster_percentiles(data, instance_size)

    # Add InstanceSize column for action labelling
    data["InstanceSize"] = instance_size

    on_start = 7
    off_end  = 18

    # --- CPU Recommendation ---
    cpu_rec = recommendations(
        "Cpu", data, hr_type, business_hour, instance_size,
        start_date, end_date, on_start, off_end
    )

    # --- Memory Recommendation ---
    mem_data = data.copy()
    # Derive MemMaxGt50/Gt25 from MemPctMax since MongoDB doesn't have them directly
    mem_data["MemMaxGt50"] = (mem_data["MemPctMax"] > 50).astype(int)
    mem_data["MemMaxGt25"] = (mem_data["MemPctMax"] > 25).astype(int)
    mem_rec = recommendations(
        "Mem", mem_data, hr_type, business_hour, instance_size,
        start_date, end_date, on_start, off_end
    )

    # --- Connections Recommendation ---
    conn_rec_tier, conn_comment = connections_recommendations(data, instance_size)

    # --- Overall recommendation ---
    current_idx = get_tier_index(instance_size)
    cpu_tier  = cpu_rec[0][0] if cpu_rec else instance_size
    mem_tier  = mem_rec[0][0] if mem_rec else instance_size
    conn_tier = conn_rec_tier

    cpu_idx  = get_tier_index(cpu_tier)
    mem_idx  = get_tier_index(mem_tier)
    conn_idx = get_tier_index(conn_tier)

    # Overall = MAX of all three (most conservative)
    overall_idx = max(cpu_idx, mem_idx, conn_idx)
    overall_tier = ATLAS_TIERS_ORDERED[overall_idx]

    # Comment
    comment = _comment(cpu_idx, mem_idx, conn_idx, current_idx)

    # Misc comment (seasonality)
    cpu_comment  = cpu_rec[0][2] if cpu_rec else ""
    mem_comment  = mem_rec[0][2] if mem_rec else ""
    misc = f"CPU:{cpu_comment} / Mem:{mem_comment} / Connections:{conn_comment}"
    misc_parsed = misc_comment(misc)

    hour_type = "Weekend" if day_type == "Weekend" else bh

    return {
        "ClusterKey": cluster_key,
        "ClusterName": cluster_name,
        "DayType": day_type,
        "HourType": hour_type,
        "CurrentSKU": instance_size,
        "CpuRec": cpu_tier,
        "MemRec": mem_tier,
        "ConnectionsRec": conn_tier,
        "OverallRecommendation": overall_tier,
        "Comment": comment,
        "MiscComment": misc_parsed,
    }


# ===========================================================================
# 14. DATABASE INSERT
# ===========================================================================

INSERT_SQL = """
    INSERT INTO [Metrics].[MongoDBRightsizingRecommendations] (
        Month, ClusterKey, ClusterName, DayType, HourType, CurrentSKU,
        OrgKey, ProjectKey, CpuRec, MemRec, ConnectionsRec,
        OverallRecommendation, Comment, MiscComment,
        ProviderName, RegionName
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_COLUMNS = [
    "Month", "ClusterKey", "ClusterName", "DayType", "HourType", "CurrentSKU",
    "OrgKey", "ProjectKey", "CpuRec", "MemRec", "ConnectionsRec",
    "OverallRecommendation", "Comment", "MiscComment",
    "ProviderName", "RegionName",
]


def insert_in_chunks(df: pd.DataFrame, chunk_size: int = 100) -> None:
    """Insert *df* into the recommendations table in batches of *chunk_size*."""
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start : start + chunk_size].copy()

        data = [
            tuple(row[col] for col in INSERT_COLUMNS)
            for _, row in chunk.iterrows()
        ]

        conn   = connect_to_db()
        cursor = conn.cursor()
        cursor.executemany(INSERT_SQL, data)
        conn.commit()
        cursor.close()
        conn.close()


# ===========================================================================
# 15. MAIN EXECUTION
# ===========================================================================

if __name__ == "__main__":

    # --- Dynamic last-month date range ---
    today       = date.today()
    last_month  = today - relativedelta(months=1)
    months      = [last_month.strftime("%Y-%m")]
    start_dates = [last_month.replace(day=1).strftime("%Y-%m-%d")]
    end_dates   = [
        ((last_month.replace(day=1) + relativedelta(months=1)) - timedelta(days=1)).strftime("%Y-%m-%d")
    ]

    print("months    =", months)
    print("StartDate =", start_dates)
    print("EndDate   =", end_dates)

    # Choose provider/region for this run (could be parameterized)
    provider = "AZURE"
    region = "US_EAST_2"
    atlas_specs, atlas_tiers = load_atlas_instance_specs(provider, region)

    # --- Build month -> config combinations ---
    types           = ["Weekday", "Weekday",         "Weekend"]
    business_hours  = ["BusinessHours", "NonBusinessHours", "BusinessHours"]
    business_hours1 = ["BusinessHours", "NonBusinessHours", "NonBusinessHours"]

    month_dict: dict = {}
    for month, start, end in zip(months, start_dates, end_dates):
        month_dict[month] = [
            {"Type": t, "BusinessHour": bh, "BusinessHour1": bh1, "StartDate": start, "EndDate": end}
            for t, bh, bh1 in zip(types, business_hours, business_hours1)
        ]

    # --- Output schema ---
    COLUMNS = [
        "Month", "ClusterKey", "ClusterName", "DayType", "HourType", "CurrentSKU",
        "OrgKey", "ProjectKey", "CpuRec", "MemRec", "ConnectionsRec",
        "OverallRecommendation", "Comment", "MiscComment",
        "ProviderName", "RegionName",
    ]

    global_df = pd.DataFrame(columns=COLUMNS)

    # --- Per-month, per-config processing ---
    for month, configs in month_dict.items():
        for config in configs:
            weekday_count, weekend_count = _count(config["StartDate"], config["EndDate"])

            # Fetch cluster inventory
            inventory_sql = build_cluster_inventory_query(config["StartDate"], config["EndDate"])
            df_clusters = fetch_data(inventory_sql)

            if df_clusters.empty:
                print(f"No clusters found for {config}")
                continue

            for _, cluster_row in df_clusters.iterrows():
                cluster_key   = cluster_row["ClusterKey"]
                cluster_name  = cluster_row["ClusterName"]
                instance_size = cluster_row["InstanceSize"]
                provider_name = cluster_row.get("ProviderName", "")
                region_name   = cluster_row.get("RegionName", "")
                org_key       = cluster_row.get("OrgKey", "")
                project_key   = cluster_row.get("ProjectKey", "")

                print(f"Processing: {cluster_name} ({instance_size}) - {config['Type']}/{config['BusinessHour']}")

                result = process_cluster(cluster_key, cluster_name, instance_size, config)

                if result is None:
                    continue

                result["Month"]        = month
                result["ProviderName"] = provider_name
                result["RegionName"]   = region_name
                result["OrgKey"]       = org_key
                result["ProjectKey"]   = project_key

                global_df = pd.concat([global_df, pd.DataFrame([result])], ignore_index=True)

    # --- Insert results ---
    if not global_df.empty:
        print(f"\nTotal recommendations: {len(global_df)}")
        print(global_df[["ClusterName", "CurrentSKU", "OverallRecommendation", "Comment"]].to_string())
        insert_in_chunks(global_df)
        print("\nResults inserted into database.")
    else:
        print("\nNo recommendations generated.")
