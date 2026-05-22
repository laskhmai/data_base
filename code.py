-- ALL 12 SOURCE TABLES + AGGREGATION IN ONE QUERY
-- Save this as one CSV

-- AGGREGATION TABLE
SELECT
    'AGGREGATION'                   AS Source,
    ProcessId                       AS [Key],
    DateTimeEST                     AS DateTime,
    CpuAvg                          AS CpuAvg,
    CpuMax                          AS CpuMax,
    MemResidentMax                  AS MemResidentMax,
    MemAvailableMin                 AS MemAvailableMin,
    NetInAvg                        AS NetInAvg,
    NetInMax                        AS NetInMax,
    NetOutAvg                       AS NetOutAvg,
    NetOutMax                       AS NetOutMax,
    NetRequestsMax                  AS NetRequestsMax,
    ConnectionsMax                  AS ConnectionsMax,
    OpcQueryMax                     AS OpcQueryMax,
    OpcInsertMax                    AS OpcInsertMax
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ProcessId IN (
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017',
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'
)
AND DateTimeEST = (
    SELECT MAX(DateTimeEST)
    FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
)

UNION ALL

-- 1. CPU AVG
SELECT
    '1-CpuAvg'      AS Source,
    [Key], DateTime,
    AVG(Measurement), NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_15M]
WHERE [Key] IN (
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017',
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'
)
AND DateTime >= DATEADD(HOUR, 5, (
    SELECT MAX(DateTimeEST)
    FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
AND DateTime < DATEADD(HOUR, 6, (
    SELECT MAX(DateTimeEST)
    FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
GROUP BY [Key], DateTime

UNION ALL

-- 2. CPU MAX
SELECT
    '2-CpuMax', [Key], DateTime,
    NULL, MAX(Measurement), NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_15M]
WHERE [Key] IN (
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017',
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'
)
AND DateTime >= DATEADD(HOUR, 5, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
AND DateTime < DATEADD(HOUR, 6, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
GROUP BY [Key], DateTime

UNION ALL

-- 3. MEMORY RESIDENT
SELECT
    '3-MemResident', [Key], DateTime,
    NULL, NULL, MAX(Measurement), NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM [Metrics].[MongoDB_Memory_Resident_5M]
WHERE [Key] IN (
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017',
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'
)
AND DateTime >= DATEADD(HOUR, 5, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
AND DateTime < DATEADD(HOUR, 6, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
GROUP BY [Key], DateTime

UNION ALL

-- 4. MEMORY AVAILABLE
SELECT
    '4-MemAvailable', [Key], DateTime,
    NULL, NULL, NULL, MIN(Measurement),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM [Metrics].[MongoDB_System_Memory_Available_15M]
WHERE [Key] IN (
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017',
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'
)
AND DateTime >= DATEADD(HOUR, 5, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
AND DateTime < DATEADD(HOUR, 6, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
GROUP BY [Key], DateTime

UNION ALL

-- 5. NETWORK IN AVG
SELECT
    '5-NetInAvg', [Key], DateTime,
    NULL, NULL, NULL, NULL,
    AVG(Measurement), NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM [Metrics].[MongoDB_System_Network_In_15M]
WHERE [Key] IN (
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017',
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'
)
AND DateTime >= DATEADD(HOUR, 5, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
AND DateTime < DATEADD(HOUR, 6, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
GROUP BY [Key], DateTime

UNION ALL

-- 6. NETWORK IN MAX
SELECT
    '6-NetInMax', [Key], DateTime,
    NULL, NULL, NULL, NULL,
    NULL, MAX(Measurement), NULL, NULL, NULL, NULL, NULL, NULL
FROM [Metrics].[MongoDB_System_Network_In_Max_15M]
WHERE [Key] IN (
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017',
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'
)
AND DateTime >= DATEADD(HOUR, 5, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
AND DateTime < DATEADD(HOUR, 6, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
GROUP BY [Key], DateTime

UNION ALL

-- 7. NETWORK OUT AVG
SELECT
    '7-NetOutAvg', [Key], DateTime,
    NULL, NULL, NULL, NULL,
    NULL, NULL, AVG(Measurement), NULL, NULL, NULL, NULL, NULL
FROM [Metrics].[MongoDB_System_Network_Out_15M]
WHERE [Key] IN (
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017',
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'
)
AND DateTime >= DATEADD(HOUR, 5, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
AND DateTime < DATEADD(HOUR, 6, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
GROUP BY [Key], DateTime

UNION ALL

-- 8. NETWORK OUT MAX
SELECT
    '8-NetOutMax', [Key], DateTime,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, MAX(Measurement), NULL, NULL, NULL, NULL
FROM [Metrics].[MongoDB_System_Network_Out_Max_15M]
WHERE [Key] IN (
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017',
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'
)
AND DateTime >= DATEADD(HOUR, 5, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
AND DateTime < DATEADD(HOUR, 6, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
GROUP BY [Key], DateTime

UNION ALL

-- 9. NETWORK REQUESTS
SELECT
    '9-NetRequests', [Key], DateTime,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, MAX(Measurement), NULL, NULL, NULL
FROM [Metrics].[MongoDB_Network_Num_Requests_15M]
WHERE [Key] IN (
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017',
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'
)
AND DateTime >= DATEADD(HOUR, 5, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
AND DateTime < DATEADD(HOUR, 6, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
GROUP BY [Key], DateTime

UNION ALL

-- 10. CONNECTIONS
SELECT
    '10-Connections', [Key], DateTime,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, MAX(Measurement), NULL, NULL
FROM [Metrics].[MongoDB_Connections_15M]
WHERE [Key] IN (
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017',
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'
)
AND DateTime >= DATEADD(HOUR, 5, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
AND DateTime < DATEADD(HOUR, 6, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
GROUP BY [Key], DateTime

UNION ALL

-- 11. OPCOUNTER QUERY
SELECT
    '11-OpcQuery', [Key], DateTime,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, MAX(Measurement), NULL
FROM [Metrics].[MongoDB_Opcounter_Query_15M]
WHERE [Key] IN (
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017',
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'
)
AND DateTime >= DATEADD(HOUR, 5, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
AND DateTime < DATEADD(HOUR, 6, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
GROUP BY [Key], DateTime

UNION ALL

-- 12. OPCOUNTER INSERT
SELECT
    '12-OpcInsert', [Key], DateTime,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, MAX(Measurement)
FROM [Metrics].[MongoDB_Opcounter_Insert_15M]
WHERE [Key] IN (
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017',
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'
)
AND DateTime >= DATEADD(HOUR, 5, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
AND DateTime < DATEADD(HOUR, 6, (
    SELECT MAX(DateTimeEST) FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
    WHERE ProcessId = 'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017'
))
GROUP BY [Key], DateTime

ORDER BY Source, [Key], DateTime