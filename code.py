-- Confirm these processes exist 
-- in aggregation table
SELECT DISTINCT
    ProcessId,
    ProcessType,
    ClusterName,
    InstanceSize,
    MIN(DateTimeEST) AS OldestHour,
    MAX(DateTimeEST) AS LatestHour,
    COUNT(*)         AS TotalRows
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ProcessId IN (
    'atlas-3w60na-shard-00-00.djfry.mongodb.net:27017',
    'atlas-ow5xth-shard-00-00.o03zm.mongodb.net:27017'
)
GROUP BY ProcessId, ProcessType, ClusterName, InstanceSize