-- STEP 1: Pick one hour from each process
DECLARE @Process1 NVARCHAR(255) = 
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
DECLARE @Process2 NVARCHAR(255) = 
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'

-- Get latest hour for each process
DECLARE @Hour1EST DATETIME
DECLARE @Hour2EST DATETIME

SELECT @Hour1EST = MAX(DateTimeEST)
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ProcessId = @Process1

SELECT @Hour2EST = MAX(DateTimeEST)
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ProcessId = @Process2

-- Convert EST to UTC (add 5 hours)
DECLARE @Hour1UTC_Start DATETIME = DATEADD(HOUR, 5, @Hour1EST)
DECLARE @Hour1UTC_End   DATETIME = DATEADD(HOUR, 1, @Hour1UTC_Start)

DECLARE @Hour2UTC_Start DATETIME = DATEADD(HOUR, 5, @Hour2EST)
DECLARE @Hour2UTC_End   DATETIME = DATEADD(HOUR, 1, @Hour2UTC_Start)

-- ─────────────────────────────────────────
-- STEP 2: Show aggregation table values
-- ─────────────────────────────────────────
SELECT
    'AGGREGATION TABLE'     AS Source,
    ProcessId,
    DateTimeEST,
    InstanceSize,
    CpuAvg,
    CpuMax,
    MemResidentMax,
    MemResidentAvg,
    MemAvailableMin,
    NetInAvg,
    NetInMax,
    NetOutAvg,
    NetOutMax,
    NetRequestsMax,
    ConnectionsMax,
    ConnectionsAvg,
    OpcQueryMax,
    OpcInsertMax
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ProcessId IN (@Process1, @Process2)
AND DateTimeEST IN (@Hour1EST, @Hour2EST)

-- ─────────────────────────────────────────
-- STEP 3: Show raw source table values
-- for Process 1
-- ─────────────────────────────────────────
UNION ALL
SELECT
    'RAW - CpuAvg'          AS Source,
    @Process1               AS ProcessId,
    @Hour1EST               AS DateTimeEST,
    NULL                    AS InstanceSize,
    AVG(Measurement)        AS CpuAvg,
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_15M]
WHERE [Key] = @Process1
AND DateTime >= @Hour1UTC_Start
AND DateTime <  @Hour1UTC_End

UNION ALL
SELECT
    'RAW - CpuMax',
    @Process1, @Hour1EST, NULL,
    NULL,
    MAX(Measurement),
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_15M]
WHERE [Key] = @Process1
AND DateTime >= @Hour1UTC_Start
AND DateTime <  @Hour1UTC_End

UNION ALL
SELECT
    'RAW - MemResidentMax',
    @Process1, @Hour1EST, NULL,
    NULL,NULL,
    MAX(Measurement),
    AVG(Measurement),
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
FROM [Metrics].[MongoDB_Memory_Resident_5M]
WHERE [Key] = @Process1
AND DateTime >= @Hour1UTC_Start
AND DateTime <  @Hour1UTC_End

UNION ALL
SELECT
    'RAW - MemAvailableMin',
    @Process1, @Hour1EST, NULL,
    NULL,NULL,NULL,NULL,
    MIN(Measurement),
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
FROM [Metrics].[MongoDB_System_Memory_Available_15M]
WHERE [Key] = @Process1
AND DateTime >= @Hour1UTC_Start
AND DateTime <  @Hour1UTC_End

UNION ALL
SELECT
    'RAW - NetInAvg',
    @Process1, @Hour1EST, NULL,
    NULL,NULL,NULL,NULL,NULL,
    AVG(Measurement),
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
FROM [Metrics].[MongoDB_System_Network_In_15M]
WHERE [Key] = @Process1
AND DateTime >= @Hour1UTC_Start
AND DateTime <  @Hour1UTC_End

UNION ALL
SELECT
    'RAW - NetInMax',
    @Process1, @Hour1EST, NULL,
    NULL,NULL,NULL,NULL,NULL,NULL,
    MAX(Measurement),
    NULL,NULL,NULL,NULL,NULL,NULL,NULL
FROM [Metrics].[MongoDB_System_Network_In_Max_15M]
WHERE [Key] = @Process1
AND DateTime >= @Hour1UTC_Start
AND DateTime <  @Hour1UTC_End

UNION ALL
SELECT
    'RAW - NetOutAvg',
    @Process1, @Hour1EST, NULL,
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,
    AVG(Measurement),
    NULL,NULL,NULL,NULL,NULL,NULL
FROM [Metrics].[MongoDB_System_Network_Out_15M]
WHERE [Key] = @Process1
AND DateTime >= @Hour1UTC_Start
AND DateTime <  @Hour1UTC_End

UNION ALL
SELECT
    'RAW - NetOutMax',
    @Process1, @Hour1EST, NULL,
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
    MAX(Measurement),
    NULL,NULL,NULL,NULL,NULL
FROM [Metrics].[MongoDB_System_Network_Out_Max_15M]
WHERE [Key] = @Process1
AND DateTime >= @Hour1UTC_Start
AND DateTime <  @Hour1UTC_End

UNION ALL
SELECT
    'RAW - NetRequestsMax',
    @Process1, @Hour1EST, NULL,
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
    MAX(Measurement),
    NULL,NULL,NULL,NULL
FROM [Metrics].[MongoDB_Network_Num_Requests_15M]
WHERE [Key] = @Process1
AND DateTime >= @Hour1UTC_Start
AND DateTime <  @Hour1UTC_End

UNION ALL
SELECT
    'RAW - ConnectionsMax',
    @Process1, @Hour1EST, NULL,
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
    MAX(Measurement),
    AVG(Measurement),
    NULL,NULL
FROM [Metrics].[MongoDB_Connections_15M]
WHERE [Key] = @Process1
AND DateTime >= @Hour1UTC_Start
AND DateTime <  @Hour1UTC_End

UNION ALL
SELECT
    'RAW - OpcQueryMax',
    @Process1, @Hour1EST, NULL,
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
    MAX(Measurement),
    NULL
FROM [Metrics].[MongoDB_Opcounter_Query_15M]
WHERE [Key] = @Process1
AND DateTime >= @Hour1UTC_Start
AND DateTime <  @Hour1UTC_End

UNION ALL
SELECT
    'RAW - OpcInsertMax',
    @Process1, @Hour1EST, NULL,
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
    MAX(Measurement)
FROM [Metrics].[MongoDB_Opcounter_Insert_15M]
WHERE [Key] = @Process1
AND DateTime >= @Hour1UTC_Start
AND DateTime <  @Hour1UTC_End

ORDER BY ProcessId, Source