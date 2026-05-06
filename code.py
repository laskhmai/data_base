-- How many rows do we have?
SELECT 
    COUNT(*)              AS TotalRows,
    COUNT(DISTINCT ProcessId)   AS UniqueProcesses,
    COUNT(DISTINCT ClusterKey)  AS UniqueClusters,
    MIN(DateTimeEST)      AS OldestHour,
    MAX(DateTimeEST)      AS LatestHour
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]


-- Critical columns should never be null
SELECT
    SUM(CASE WHEN ProcessId    IS NULL THEN 1 ELSE 0 END) AS NullProcessId,
    SUM(CASE WHEN ClusterKey   IS NULL THEN 1 ELSE 0 END) AS NullClusterKey,
    SUM(CASE WHEN DateTimeEST  IS NULL THEN 1 ELSE 0 END) AS NullDateTimeEST,
    SUM(CASE WHEN InstanceSize IS NULL THEN 1 ELSE 0 END) AS NullInstanceSize,
    SUM(CASE WHEN ProcessType  IS NULL THEN 1 ELSE 0 END) AS NullProcessType,
    SUM(CASE WHEN ProviderName IS NULL THEN 1 ELSE 0 END) AS NullProviderName,
    SUM(CASE WHEN RegionName   IS NULL THEN 1 ELSE 0 END) AS NullRegionName
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]


-- Network columns were previously missing
-- Make sure they are now coming in
SELECT
    SUM(CASE WHEN NetInAvg      = 0 THEN 1 ELSE 0 END) AS NetInAvgZero,
    SUM(CASE WHEN NetInMax      = 0 THEN 1 ELSE 0 END) AS NetInMaxZero,
    SUM(CASE WHEN NetOutAvg     = 0 THEN 1 ELSE 0 END) AS NetOutAvgZero,
    SUM(CASE WHEN NetOutMax     = 0 THEN 1 ELSE 0 END) AS NetOutMaxZero,
    SUM(CASE WHEN NetRequestsMax= 0 THEN 1 ELSE 0 END) AS NetRequestsZero,
    COUNT(*)                                            AS TotalRows
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]



-- Confirm both process types exist
SELECT 
    ProcessType,
    COUNT(*)              AS RowCount,
    COUNT(DISTINCT ProcessId) AS UniqueProcesses
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
GROUP BY ProcessType


-- Make sure JSON parsing is working
SELECT 
    InstanceSize,
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
GROUP BY InstanceSize
ORDER BY RowCount DESC



-- There should be zero duplicates
SELECT 
    ProcessId,
    DateTimeEST,
    _date,
    _hour,
    [type],
    businessHour,
    COUNT(*) AS DuplicateCount
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
GROUP BY 
    ProcessId, DateTimeEST, _date, _hour, [type], businessHour
HAVING COUNT(*) > 1




-- Pick latest hour and validate CpuMax against raw table
DECLARE @ProcessId  NVARCHAR(255) = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
DECLARE @DateEST    DATETIME      = '2026-05-02 06:00:00'
DECLARE @DateUTC    DATETIME      = '2026-05-02 11:00:00' -- EST + 5hrs

-- Aggregation table value
SELECT 'Aggregation' AS Source, CpuMax, CpuAvg, MemResidentMax, ConnectionsMax
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ProcessId  = @ProcessId
AND DateTimeEST  = @DateEST

UNION ALL

-- Raw source value
SELECT 'Raw Source', 
    MAX(Measurement),
    AVG(Measurement),
    NULL,
    NULL
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_15M]
WHERE [Key]    = @ProcessId
AND DateTime  >= @DateUTC
AND DateTime  < DATEADD(HOUR, 1, @DateUTC)



-- Verify type classification is correct
SELECT 
    _date,
    DATENAME(WEEKDAY, _date) AS DayName,
    [type],
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
GROUP BY _date, DATENAME(WEEKDAY, _date), [type]
ORDER BY _date


-- Hours 7-18 = BusinessHours, rest = NonBusinessHours
SELECT 
    _hour,
    businessHour,
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
GROUP BY _hour, businessHour
ORDER BY _hour


-- Clean summary to share with manager
SELECT
    MIN(_date)                              AS DataFrom,
    MAX(_date)                              AS DataTo,
    COUNT(DISTINCT ProcessId)               AS TotalProcesses,
    COUNT(DISTINCT ClusterKey)              AS TotalClusters,
    COUNT(DISTINCT InstanceSize)            AS DistinctTiers,
    COUNT(*)                                AS TotalHourlyRows,
    ROUND(AVG(CpuMax), 2)                  AS AvgPeakCPU,
    ROUND(AVG(MemResidentMax), 2)          AS AvgMemResidentMB,
    ROUND(AVG(ConnectionsMax), 2)          AS AvgPeakConnections
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]



SELECT DISTINCT
    cl.Name,
    cl.ReplicationSpecs
FROM [Metrics].[MongoDBRightsizingAggregatedHourly] h
JOIN [MongoDB].[Clusters] cl
    ON cl.ClustersKey = h.ClusterKey
WHERE h.InstanceSize IS NULL



-- Check if table has data
SELECT 
    MIN(DateTime) AS OldestRecord,
    MAX(DateTime) AS LatestRecord,
    COUNT(*)      AS TotalRows
FROM [Metrics].[MongoDB_System_Network_Out_Max_15M]
WHERE DateTime >= '2026-05-01'
AND   DateTime <  '2026-05-04'


SELECT
    cl.Name,
    COALESCE(
        JSON_VALUE(cl.ReplicationSpecs,
            '$[0].regionConfigs[0].effectiveElectableSpecs.instanceSize'),
        JSON_VALUE(cl.ReplicationSpecs,
            '$[0].regionConfigs[0].electableSpecs.instanceSize')
    ) AS InstanceSize,
    JSON_VALUE(cl.ReplicationSpecs,
        '$[0].regionConfigs[0].providerName') AS ProviderName,
    JSON_VALUE(cl.ReplicationSpecs,
        '$[0].regionConfigs[0].regionName')   AS RegionName
FROM [MongoDB].[Clusters] cl
WHERE cl.Name IN (
    'cwth-ptscheduling-uat',
    'liquibase-mongodb-dev1',
    'cwth-ptscheduling-prod',
    'cwth-ptscheduling-qa'


    -- Find exact names for cwth clusters
SELECT ClustersKey, Name
FROM [MongoDB].[Clusters]
WHERE Name LIKE '%cwth%'
   OR Name LIKE '%scheduling%'
)




SELECT
    cl.Name,
    COALESCE(
        JSON_VALUE(cl.ReplicationSpecs,
            '$[0].regionConfigs[0].effectiveElectableSpecs.instanceSize'),
        JSON_VALUE(cl.ReplicationSpecs,
            '$[0].regionConfigs[0].electableSpecs.instanceSize')
    ) AS InstanceSize
FROM [MongoDB].[Clusters] cl
WHERE cl.Name LIKE '%cwih%'





SELECT
    MIN(DateTime) AS OldestRecord,
    MAX(DateTime) AS LatestRecord,
    COUNT(*)      AS TotalRows
FROM [Metrics].[MongoDB_System_Network_Out_Max_15M]
WHERE DateTime >= '2026-05-01'
AND   DateTime <  '2026-05-04'




SELECT
    ClustersKey,
    Name,
    ClusterId,
    CreateDate,
    StateName
FROM [MongoDB].[Clusters]
WHERE Name LIKE '%cwih%'
ORDER BY Name, CreateDate






# Run this in your notebook

# Step 1: Get Silver records NOT in Gold
sql_silver = """
    SELECT ResourceId, AccountName
    FROM [Silver].[AzureResourcesNormalized]
"""

sql_gold = """
    SELECT resource_id
    FROM [Gold].[AzureActiveResourceOwnerShipNormalized]
"""

sql_subscriptions = """
    SELECT DISTINCT [SubscriptionName]
    FROM [AZURE].[Subscriptions]
"""

# Fetch from Lenticular (Silver)
with connect(lenticular_server, lenticular_database,
             lenticular_username, 
             lenticular_password) as con_l:
    silver_df = read_sql_df(con_l, sql_silver)
    subs_df = read_sql_df(con_l, sql_subscriptions)

# Fetch from Hybrideasi (Gold)
with connect(hybrideasi_server, hybrideasi_database,
             hybrideasi_username,
             hybrideasi_password) as con_h:
    gold_df = read_sql_df(con_h, sql_gold)

# Step 2: Normalize IDs
silver_df["resource_id_norm"] = (
    silver_df["ResourceId"]
    .astype(str).str.lower().str.strip()
)
gold_df["resource_id_norm"] = (
    gold_df["resource_id"]
    .astype(str).str.lower().str.strip()
)
subs_df["sub_norm"] = (
    subs_df["SubscriptionName"]
    .astype(str).str.lower().str.strip()
)
silver_df["account_norm"] = (
    silver_df["AccountName"]
    .astype(str).str.lower().str.strip()
)

# Step 3: Find 1314 missing records
gold_ids = set(gold_df["resource_id_norm"])
missing_df = silver_df[
    ~silver_df["resource_id_norm"]
    .isin(gold_ids)
].copy()

print(f"Total missing records: {len(missing_df)}")

# Step 4: Which subscriptions?
print("\nMissing by subscription:")
print(
    missing_df.groupby("AccountName")
    .size()
    .reset_index(name="count")
    .sort_values("count", ascending=False)
    .to_string()
)

# Step 5: Are these subscriptions
# in get_subscriptions() list?
sub_list = set(subs_df["sub_norm"])
missing_df["in_subscription_list"] = (
    missing_df["account_norm"]
    .isin(sub_list)
)

print("\nIn subscription list?")
print(
    missing_df["in_subscription_list"]
    .value_counts()
)

# Step 6: Show subscriptions NOT in list
not_in_list = missing_df[
    ~missing_df["in_subscription_list"]
]["AccountName"].unique()

print("\nSubscriptions NOT in list:")
print(not_in_list)