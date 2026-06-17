"""
ult, ignore_index=True)

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
                    metacache: dict) -> dict | None:

    start_date = config["StartDate"]
    end_date   = config["EndDate"]
    day_type   = config["Type"]
    bh         = config["BusinessHour"]
    bh1        = config["BusinessHour1"]

    meta_key = (str(provider_name).upper(), str(region_name).upper())
    if meta_key not in metacache:
        print(f"Loading MetaConfig for Provider={provider}, Region={region}")
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

    cpu_rec               = recommendations("Cpu", data, actual_sku, ordered_tiers)
    mem_rec               = recommendations("Mem", data, actual_sku, ordered_tiers)
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

    current_cost           = specs.get(actual_sku,   {}).get("CostPrHour", 0.0)
    recommended_cost       = specs.get(overall_sku,  {}).get("CostPrHour", 0.0)
    hours_in_month         = _hours_in_range(start_date, end_date)
    spend_30_days          = current_cost * hours_in_month
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

    print(f"{cluster_name}")

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

    for start in range(0, total, chunk_size):
        chunk = df.iloc[start : start + chunk_size].copy()
        conn  = connect_to_db()
        cur   = conn.cursor()

        for _, row in chunk.iterrows():
            # Delete existing row for this key
            cur.execute("""
                DELETE FROM [Metrics].[MongoDBRightsizingRecommendations]
                WHERE Month      = ?
                AND   ClusterKey = ?
                AND   DayType    = ?
                AND   HourType   = ?
            """, row["Month"], row["ClusterKey"], row["DayType"], row["HourType"])

            # Insert fresh row
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
    # Auto-detect date range from aggregated table
    date_sql = """
        SELECT
            MIN(_date) AS MinDate,
            MAX(_date) AS MaxDate
        FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    """
    # Auto-detect date range
    date_sql = """
        SELECT
            MIN(_date) AS MinDate,
            MAX(_date) AS MaxDate
        FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    """
    date_df    = fetch_data(date_sql)
    start_dt   = pd.to_datetime(date_df["MinDate"].iloc[0])
    end_dt     = pd.to_datetime(date_df["MaxDate"].iloc[0])

    months      = [end_dt.strftime("%Y-%m")]
    start_dates = [start_dt.strftime("%Y-%m-%d")]
    end_dates   = [end_dt.strftime("%Y-%m-%d")]

    print("months =", months)
    print("StartDate =", start_dates[0])
    print("EndDate =", end_dates[0])

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