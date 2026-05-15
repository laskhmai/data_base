-- Get a recent row to validate
SELECT TOP 1
    ProcessId,
    DateTimeEST,
    _date,
    _hour,
    CpuAvg,
    CpuMax,
    MemResidentMax,
    NetInMax,
    NetOutMax,
    ConnectionsMax
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
ORDER BY DateTimeEST DESC

-- EST hour to UTC = add 5 hours
-- If DateTimeEST = 2026-05-02 06:00
-- Then UTC range = 11:00 to 12:00

DECLARE @ProcessId  NVARCHAR(255) = 'atlas-xxx.mongodb.net:27017'
DECLARE @UTCStart   DATETIME      = '2026-05-02 11:00:00'
DECLARE @UTCEnd     DATETIME      = '2026-05-02 12:00:00'

-- CPU AVG
SELECT 'CpuAvg' AS Metric,
    AVG(Measurement) AS RawCalculation
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_15M]
WHERE [Key] = @ProcessId
AND DateTime >= @UTCStart AND DateTime < @UTCEnd

UNION ALL

-- CPU MAX
SELECT 'CpuMax',
    MAX(Measurement)
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_15M]
WHERE [Key] = @ProcessId
AND DateTime >= @UTCStart AND DateTime < @UTCEnd

UNION ALL

-- MEMORY RESIDENT MAX
SELECT 'MemResidentMax',
    MAX(Measurement)
FROM [Metrics].[MongoDB_Memory_Resident_5M]
WHERE [Key] = @ProcessId
AND DateTime >= @UTCStart AND DateTime < @UTCEnd

UNION ALL

-- NETWORK IN MAX
SELECT 'NetInMax',
    MAX(Measurement)
FROM [Metrics].[MongoDB_System_Network_In_Max_15M]
WHERE [Key] = @ProcessId
AND DateTime >= @UTCStart AND DateTime < @UTCEnd

UNION ALL

-- NETWORK OUT MAX
SELECT 'NetOutMax',
    MAX(Measurement)
FROM [Metrics].[MongoDB_System_Network_Out_Max_15M]
WHERE [Key] = @ProcessId
AND DateTime >= @UTCStart AND DateTime < @UTCEnd

UNION ALL

-- CONNECTIONS MAX
SELECT 'ConnectionsMax',
    MAX(Measurement)
FROM [Metrics].[MongoDB_Connections_15M]
WHERE [Key] = @ProcessId
AND DateTime >= @UTCStart AND DateTime < @UTCEnd