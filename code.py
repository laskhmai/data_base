-- STEP 1: See the raw duplicate rows for cdr-uat
SELECT
    ClusterKey,
    ClusterName,
    ProcessId,
    _date,
    _hour,
    CpuMax,
    MemResidentMaxPct,
    ConnectionsMax,
    ConnUtilizationPct
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ClusterKey  = 330
AND   ProcessType = 'REPLICA_PRIMARY'
AND   _date       = '2026-05-20'
AND   _hour       = 8
ORDER BY ProcessId
GO

-- STEP 2: What the fixed version would produce
SELECT
    ClusterKey,
    ClusterName,
    _date,
    _hour,
    COUNT(DISTINCT ProcessId)  AS ShardCount,
    MAX(CpuMax)                AS CpuMax,
    MAX(MemResidentMaxPct)     AS MemResidentMaxPct,
    SUM(ConnectionsMax)        AS ConnectionsMax,
    MAX(ConnUtilizationPct)    AS ConnUtilizationPct
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ClusterKey  = 330
AND   ProcessType = 'REPLICA_PRIMARY'
AND   _date       = '2026-05-20'
AND   _hour       = 8
GROUP BY
    ClusterKey,
    ClusterName,
    _date,
    _hour
GO