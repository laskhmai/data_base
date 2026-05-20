# =============================================
# MongoDB Rightsizing Notebook
# Project: COSD Team — Humana
# Azure DevOps: 9009227
# =============================================

# =============================================
# CELL 1 — Imports & Connection
# =============================================

import pyodbc
import pandas as pd
import numpy as np
import json
import warnings
from datetime import datetime, timedelta
from sklearn.cluster import KMeans
from statsmodels.tsa.seasonal import STL

warnings.filterwarnings("ignore")


def connect_to_db():
    try:
        conn = pyodbc.connect(
            f'DRIVER={driver};SERVER={server};'
            f'DATABASE={database};UID={username};PWD={password};'
        )
        return conn
    except Exception as e:
        print(f"Connection error: {e}")
        exit(1)

def fetch_data(sql):
    try:
        conn = connect_to_db()
        data = pd.read_sql(sql, conn)
        conn.close()
        return data
    except Exception as e:
        print(f"Fetch error: {e}")
        return pd.DataFrame()

print("✅ Cell 1 done — Connection ready!")

# =============================================
# CELL 2 — SKU Details from MetaConfig
# =============================================

sku_details_query = """
SELECT
    SkuName,
    Tier,
    vCores,
    MemorySizeGB,
    Instance,
    CostPrHour,
    Provider,
    Region
FROM [Analytics].[MongoDBMetaConfig]
WHERE Tier NOT IN ('Free','Flex')
ORDER BY Provider, Region, Tier, MemorySizeGB
"""

sku_details_df = fetch_data(sku_details_query)
sku_details_df['SkuName'] = sku_details_df['SkuName'].str.upper()

print(f"✅ Cell 2 done — SKU details loaded: {len(sku_details_df)} rows")
print(sku_details_df.head())

# =============================================
# CELL 3 — Get All Clusters to Process
# =============================================

clusters_query = """
SELECT DISTINCT
    h.ClusterKey,
    h.ClusterName,
    h.InstanceSize              AS ActualSku,
    h.ProviderName,
    h.RegionName,
    o.Name                      AS OrgName,
    p.ProjectKey,
    m.Tier,
    m.CostPrHour                AS CurrentCostPrHour,
    m.MemorySizeGB              AS TierMemoryGB
FROM [Metrics].[MongoDBRightsizingAggregatedHourly] h
JOIN [MongoDB].[Process]           p ON p.ProcessId  = h.ProcessId
JOIN [MongoDB].[Organization]      o ON o.OrgKey     = p.OrgKey
JOIN [Analytics].[MongoDBMetaConfig] m
    ON  m.SkuName   = h.InstanceSize
    AND m.Provider  = h.ProviderName
    AND m.Region    = h.RegionName
    AND m.Tier      NOT IN ('Free','Flex','Burstable')
WHERE h.InstanceSize IS NOT NULL
AND   h.InstanceSize NOT IN ('M0','M2','M5')
AND   h.ProcessType  = 'REPLICA_PRIMARY'
"""

clusters_df = fetch_data(clusters_query)
clusters_df = clusters_df.drop_duplicates(subset=['ClusterKey'])

print(f"✅ Cell 3 done — Total clusters: {len(clusters_df)}")
print(clusters_df[['ClusterName','ActualSku','ProviderName',
                    'RegionName','Tier','CurrentCostPrHour']].head(10))

# =============================================
# CELL 4 — Query Function
# =============================================

def query(ClusterKey, StartDate, EndDate):

    sql = f"""
    SELECT
        h.ClusterKey,
        h.ClusterName,
        h.InstanceSize              AS CurrentSku,
        h._date                     AS [Date],
        h._hour                     AS [Hour],
        h.[type]                    AS DayType,
        h.businessHour,
        h.CpuAvg,
        h.CpuMax,
        -- Pre-calculated threshold counts from aggregation
        COALESCE(h.CpuMaxGt50, 0)   AS CpuMaxGt50,
        COALESCE(h.CpuMaxGt25, 0)   AS CpuMaxGt25,
        COALESCE(h.CpuMaxGt10, 0)   AS CpuMaxGt10,
        h.MemResidentMax,
        h.MemResidentAvg,
        h.MemAvailableMin,
        h.ConnectionsMax,
        h.ConnectionsAvg,
        h.NetInMax,
        h.NetOutMax,
        h.OpcQueryMax,
        h.OpcInsertMax
    FROM [Metrics].[MongoDBRightsizingAggregatedHourly] h
    WHERE h.ClusterKey  = {ClusterKey}
    AND   h._date       BETWEEN '{StartDate}' AND '{EndDate}'
    AND   h.ProcessType = 'REPLICA_PRIMARY'
    AND   h.InstanceSize IS NOT NULL
    ORDER BY h._date, h._hour
    """

    return fetch_data(sql)

print("✅ Cell 4 done — Query function ready!")

# =============================================
# CELL 5 — CPU Recommendation
# =============================================

def cpu_recommendation(data):

    if data.empty:
        return 'Keep', 0.0, 0.0, 0, 0, 0

    avg_cpu      = round(float(data['CpuMax'].mean()), 2)
    peak_cpu     = round(float(data['CpuMax'].max()),  2)

    # Use pre-calculated columns from aggregation table
    hrs_above_50 = int(data['CpuMaxGt50'].sum())
    hrs_above_25 = int(data['CpuMaxGt25'].sum())
    hrs_above_10 = int(data['CpuMaxGt10'].sum())

    # L3 = ScaleUp
    if peak_cpu > 50 or hrs_above_50 > 0:
        rec = 'ScaleUp'
    # L1 = ScaleDown
    elif avg_cpu < 25 and hrs_above_25 == 0:
        rec = 'ScaleDown'
    # L2 = Keep
    else:
        rec = 'Keep'

    return rec, avg_cpu, peak_cpu, hrs_above_50, hrs_above_25, hrs_above_10

print("✅ Cell 5 done — CPU recommendation function ready!")

# =============================================
# CELL 6 — Memory Recommendation
# =============================================

def mem_recommendation(data, tier_ram_gb):

    if data.empty:
        return 'Keep', 0.0, 0.0

    # MemResidentMax is already in MB from aggregation table
    tier_ram_mb  = tier_ram_gb * 1024
    peak_mem_mb  = float(data['MemResidentMax'].max())
    mem_util_pct = round((peak_mem_mb / tier_ram_mb) * 100, 2) \
                   if tier_ram_mb > 0 else 0.0

    # L3 = ScaleUp — memory > 70% of tier RAM
    if mem_util_pct > 70:
        rec = 'ScaleUp'
    # L1 = ScaleDown — memory < 30% of tier RAM
    elif mem_util_pct < 30:
        rec = 'ScaleDown'
    # L2 = Keep
    else:
        rec = 'Keep'

    return rec, mem_util_pct, peak_mem_mb

print("✅ Cell 6 done — Memory recommendation function ready!")

# =============================================
# CELL 7 — Connection Recommendation
# =============================================

def conn_recommendation(data, sku_name):

    # Connection limits per tier
    conn_limits = {
        'M10' : 1500,  'M20' : 3000,
        'M30' : 6000,  'M40' : 16000,
        'M50' : 32000, 'M60' : 64000,
        'M80' : 96000, 'M200': 128000,
        'M300': 128000,'M400': 128000
    }

    base_sku  = sku_name.upper().split()[0]
    limit     = conn_limits.get(base_sku, 16000)

    if data.empty:
        return 'Keep', 0.0

    peak_conn    = float(data['ConnectionsMax'].max())
    conn_util    = round((peak_conn / limit) * 100, 2) \
                   if limit > 0 else 0.0

    # L3 = ScaleUp — connections > 70% of limit
    if conn_util > 70:
        rec = 'ScaleUp'
    else:
        rec = 'Keep'

    return rec, conn_util

print("✅ Cell 7 done — Connection recommendation function ready!")

# =============================================
# CELL 8 — Trend Analysis (MiscComment)
# =============================================

def get_trend(data):
    try:
        if len(data) < 14:
            return "Insufficient data for trend analysis"

        series = data.groupby('Date')['CpuMax'].mean()

        if len(series) < 7:
            return "Less than 7 days data — no trend"

        stl    = STL(series, period=7)
        result = stl.fit()
        trend  = result.trend

        if trend.iloc[-1] > trend.iloc[0] * 1.05:
            return "Increasing trend observed in usage"
        elif trend.iloc[-1] < trend.iloc[0] * 0.95:
            return "Decreasing trend observed in usage"
        else:
            return "No significant trend observed"
    except Exception as e:
        return "No trend observed"

print("✅ Cell 8 done — Trend function ready!")

# =============================================
# CELL 9 — Comment Builder
# =============================================

def build_comment(cpu_rec, mem_rec, conn_rec,
                  avg_cpu, mem_util, conn_util):
    parts = []

    # CPU
    if cpu_rec == 'ScaleDown':
        parts.append(f"CPU Underutilized ({avg_cpu}% avg)")
    elif cpu_rec == 'ScaleUp':
        parts.append(f"CPU Intensive ({avg_cpu}% avg)")
    else:
        parts.append(f"CPU Optimal Usage ({avg_cpu}% avg)")

    # Memory
    if mem_rec == 'ScaleDown':
        parts.append(f"Memory Underutilized ({mem_util}% of tier RAM)")
    elif mem_rec == 'ScaleUp':
        parts.append(f"Memory Intensive ({mem_util}% of tier RAM)")
    else:
        parts.append(f"Memory Optimal Usage ({mem_util}% of tier RAM)")

    # Connections (only mention if high)
    if conn_rec == 'ScaleUp':
        parts.append(f"High Connection Usage ({conn_util}% of limit)")

    return "; ".join(parts)

print("✅ Cell 9 done — Comment builder ready!")

# =============================================
# CELL 10 — Efficiency Calculator
# =============================================

def calculate_current_efficiency(avg_cpu, mem_util, conn_util):
    return json.dumps({
        "CpuEfficiency" : str(round(avg_cpu   / 100, 4)),
        "MemEfficiency" : str(round(mem_util  / 100, 4)),
        "ConnEfficiency": str(round(conn_util / 100, 4))
    })

def calculate_within_efficiency(avg_cpu, peak_mem_mb,
                                 rec_tier_ram_gb, conn_util):
    rec_ram_mb      = rec_tier_ram_gb * 1024
    rec_mem_util    = round((peak_mem_mb / rec_ram_mb) * 100, 2) \
                      if rec_ram_mb > 0 else 0.0

    return json.dumps({
        "CpuEfficiency" : str(round(avg_cpu      / 100, 4)),
        "MemEfficiency" : str(round(rec_mem_util / 100, 4)),
        "ConnEfficiency": str(round(conn_util    / 100, 4))
    })

print("✅ Cell 10 done — Efficiency calculator ready!")

# =============================================
# CELL 11 — Final Recommendation
# =============================================

def final_recommendation(cpu_rec, mem_rec, conn_rec,
                          actual_sku, tier,
                          provider, region,
                          sku_details_df):

    # ANY ScaleUp → ScaleUp
    if 'ScaleUp' in [cpu_rec, mem_rec, conn_rec]:
        action = 'ScaleUp'
    # ALL ScaleDown → ScaleDown
    elif all(r == 'ScaleDown' for r in [cpu_rec, mem_rec]):
        action = 'ScaleDown'
    # Otherwise → Keep
    else:
        action = 'Keep'

    # Get same tier SKUs ordered by memory
    same_tier = sku_details_df[
        (sku_details_df['Tier']     == tier)     &
        (sku_details_df['Provider'] == provider) &
        (sku_details_df['Region']   == region)
    ].sort_values('MemorySizeGB').reset_index(drop=True)

    if same_tier.empty:
        return (f"{actual_sku} {tier}",
                0.0, 0.0, 'Optimal')

    # Find current SKU position
    current_rows = same_tier[
        same_tier['SkuName'] == actual_sku.upper()
    ]

    if current_rows.empty:
        return (f"{actual_sku} {tier}",
                0.0, 0.0, 'Optimal')

    current_idx = current_rows.index[0]

    # Get recommended SKU
    if action == 'ScaleUp':
        if current_idx < len(same_tier) - 1:
            rec_row = same_tier.loc[current_idx + 1]
        else:
            rec_row = same_tier.loc[current_idx]
            action  = 'Keep'

    elif action == 'ScaleDown':
        if current_idx > 0:
            rec_row = same_tier.loc[current_idx - 1]
        else:
            rec_row = same_tier.loc[current_idx]
            action  = 'Keep'
    else:
        rec_row = same_tier.loc[current_idx]

    rec_sku      = f"{rec_row['SkuName']} {rec_row['Tier']}"
    rec_cost     = float(rec_row['CostPrHour'])
    rec_mem_gb   = float(rec_row['MemorySizeGB'])
    final_action = 'Optimal' if action == 'Keep' else 'Rightsize'

    return rec_sku, rec_cost, rec_mem_gb, final_action

print("✅ Cell 11 done — Final recommendation function ready!")

# =============================================
# CELL 12 — Main Processing Loop
# =============================================

# Date range
StartDate = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
EndDate   = datetime.now().strftime('%Y-%m-%d')
Month     = datetime.now().strftime('%Y-%m')

print(f"Analysis period : {StartDate} to {EndDate}")
print(f"Month           : {Month}")
print(f"Total clusters  : {len(clusters_df)}")
print("-" * 60)

global_df = pd.DataFrame()

for idx, cluster in clusters_df.iterrows():

    ClusterKey  = cluster['ClusterKey']
    ClusterName = cluster['ClusterName']
    ActualSku   = str(cluster['ActualSku']).upper()
    Provider    = cluster['ProviderName']
    Region      = cluster['RegionName']
    OrgName     = cluster['OrgName']
    ProjectKey  = cluster['ProjectKey']
    Tier        = cluster['Tier']
    CurrCost    = float(cluster['CurrentCostPrHour'])
    TierMemGB   = float(cluster['TierMemoryGB'])

    print(f"Processing [{idx+1}/{len(clusters_df)}]: "
          f"{ClusterName} | {ActualSku} {Tier} | {Provider}")

    # Fetch cluster data
    data = query(ClusterKey, StartDate, EndDate)

    if data.empty:
        print(f"  ⚠️  No data found — skipping")
        continue

    # --- CPU ---
    cpu_rec, avg_cpu, peak_cpu, \
    hrs_50, hrs_25, hrs_10 = cpu_recommendation(data)

    # --- Memory ---
    mem_rec, mem_util, peak_mem_mb = mem_recommendation(
        data, TierMemGB
    )

    # --- Connections ---
    conn_rec, conn_util = conn_recommendation(data, ActualSku)

    # --- Trend ---
    misc_comment = get_trend(data)

    # --- Comment ---
    comment = build_comment(
        cpu_rec, mem_rec, conn_rec,
        avg_cpu, mem_util, conn_util
    )

    # --- Final Recommendation ---
    rec_sku, rec_cost, rec_mem_gb, action = final_recommendation(
        cpu_rec, mem_rec, conn_rec,
        ActualSku, Tier, Provider, Region,
        sku_details_df
    )

    # --- Savings ---
    est_savings = round((CurrCost - rec_cost) * 24 * 30, 2) \
                  if action == 'Rightsize' and rec_cost < CurrCost \
                  else 0.0

    # --- Efficiency ---
    curr_eff   = calculate_current_efficiency(
        avg_cpu, mem_util, conn_util
    )
    within_eff = calculate_within_efficiency(
        avg_cpu, peak_mem_mb, rec_mem_gb, conn_util
    )

    # --- Build Row ---
    new_row = pd.DataFrame([{
        'Month'                        : Month,
        'ClusterKey'                   : int(ClusterKey),
        'ClusterName'                  : ClusterName,
        'OrgName'                      : OrgName,
        'ProjectKey'                   : int(ProjectKey),
        'ProviderName'                 : Provider,
        'RegionName'                   : Region,
        'CurrentSku'                   : f"{ActualSku} {Tier}",
        'CurrentCostPrHour'            : CurrCost,
        'CpuRec'                       : cpu_rec,
        'MemRec'                       : mem_rec,
        'ConnRec'                      : conn_rec,
        'AvgCpuMax'                    : avg_cpu,
        'PeakCpuMax'                   : peak_cpu,
        'MemUtilizationPct'            : mem_util,
        'ConnUtilizationPct'           : conn_util,
        'RecommendedSku'               : rec_sku,
        'RecommendedCostPrHour'        : rec_cost,
        'EstimatedMonthlySavings'      : est_savings,
        'Comment'                      : comment,
        'MiscComment'                  : misc_comment,
        'CurrentEfficiency'            : curr_eff,
        'WithinEfficiency'             : within_eff,
        'OutsideEfficiency'            : None,
        'Spend30days'                  : None,
        'WithinFamilySavings'          : est_savings,
        'OverallDifferentVersionSavings': 0.0,
        'Action'                       : action,
        'AuditUtc'                     : datetime.utcnow()
    }])

    global_df = pd.concat([global_df, new_row],
                           ignore_index=True)

    print(f"  → {action} | "
          f"{ActualSku} {Tier} (${CurrCost}/hr) → "
          f"{rec_sku} (${rec_cost}/hr) | "
          f"Save ${est_savings}/month")

print("-" * 60)
print(f"\n✅ Cell 12 done!")
print(f"Total recommendations generated: {len(global_df)}")
print(f"\nSummary:")
print(global_df.groupby('Action')['ClusterName'].count())
print(f"\nTop savings:")
print(global_df[['ClusterName','CurrentSku','RecommendedSku',
                  'Action','EstimatedMonthlySavings']]
      .sort_values('EstimatedMonthlySavings', ascending=False)
      .head(10))

# =============================================
# CELL 13 — Insert to Database
# =============================================

sql_insert = """
INSERT INTO [Metrics].[MongoDBRightsizingRecommendations]
(
    Month, ClusterKey, ClusterName,
    OrgName, ProjectKey,
    ProviderName, RegionName,
    CurrentSku, CurrentCostPrHour,
    CpuRec, MemRec, ConnRec,
    AvgCpuMax, PeakCpuMax,
    MemUtilizationPct, ConnUtilizationPct,
    RecommendedSku, RecommendedCostPrHour,
    EstimatedMonthlySavings,
    Comment, MiscComment,
    CurrentEfficiency, WithinEfficiency,
    OutsideEfficiency,
    Spend30days, WithinFamilySavings,
    OverallDifferentVersionSavings,
    Action, AuditUtc
)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

def insert_in_chunks(df, chunk_size=50):
    total = 0
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start + chunk_size].copy()

        data = [
            (
                row['Month'],
                row['ClusterKey'],
                row['ClusterName'],
                row['OrgName'],
                row['ProjectKey'],
                row['ProviderName'],
                row['RegionName'],
                row['CurrentSku'],
                row['CurrentCostPrHour'],
                row['CpuRec'],
                row['MemRec'],
                row['ConnRec'],
                row['AvgCpuMax'],
                row['PeakCpuMax'],
                row['MemUtilizationPct'],
                row['ConnUtilizationPct'],
                row['RecommendedSku'],
                row['RecommendedCostPrHour'],
                row['EstimatedMonthlySavings'],
                row['Comment'],
                row['MiscComment'],
                row['CurrentEfficiency'],
                row['WithinEfficiency'],
                row['OutsideEfficiency'],
                row['Spend30days'],
                row['WithinFamilySavings'],
                row['OverallDifferentVersionSavings'],
                row['Action'],
                row['AuditUtc']
            )
            for _, row in chunk.iterrows()
        ]

        conn   = connect_to_db()
        cursor = conn.cursor()
        cursor.executemany(sql_insert, data)
        conn.commit()
        cursor.close()
        conn.close()

        total += len(chunk)
        print(f"  Inserted {total}/{len(df)} rows...")

print("Starting insert...")
insert_in_chunks(global_df, chunk_size=50)
print(f"\n✅ Cell 13 done! {len(global_df)} rows inserted!")

# =============================================
# CELL 14 — Verify Results
# =============================================

verify_sql = f"""
SELECT
    Action,
    COUNT(*)                             AS Clusters,
    ROUND(AVG(EstimatedMonthlySavings),2) AS AvgMonthlySavings,
    ROUND(SUM(EstimatedMonthlySavings),2) AS TotalMonthlySavings,
    MIN(AuditUtc)                         AS RunTime
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month = '{Month}'
GROUP BY Action
ORDER BY Action
"""

results = fetch_data(verify_sql)
print("\n✅ Cell 14 done — Final Results:")
print("=" * 60)
print(results.to_string(index=False))
print("=" * 60)

top_savings = fetch_data(f"""
SELECT TOP 10
    ClusterName,
    CurrentSku,
    RecommendedSku,
    Action,
    EstimatedMonthlySavings,
    Comment
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month = '{Month}'
ORDER BY EstimatedMonthlySavings DESC
""")

print("\nTop 10 Savings Opportunities:")
print(top_savings.to_string(index=False))