-- How many PRIMARY processes per cluster?
SELECT
    h.ClusterKey,
    h.ClusterName,
    h.InstanceSize,
    COUNT(DISTINCT h.ProcessId) AS PrimaryProcessCount,
    STRING_AGG(h.ProcessId, ' | ') AS ProcessIds
FROM [Metrics].[MongoDBRightsizingAggregatedHourly] h
WHERE h.ProcessType = 'REPLICA_PRIMARY'
AND   h._date       = CAST(GETDATE() AS DATE)
GROUP BY
    h.ClusterKey,
    h.ClusterName,
    h.InstanceSize
ORDER BY PrimaryProcessCount DESC
GO

-- Show clusters with MORE than 1 primary
SELECT
    ClusterKey,
    ClusterName,
    _date,
    _hour,
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ProcessType = 'REPLICA_PRIMARY'
GROUP BY
    ClusterKey,
    ClusterName,
    _date,
    _hour
HAVING COUNT(*) > 1
ORDER BY RowCount DESC
GO


SELECT
    ClusterKey,
    ClusterName,
    MAX(InstanceSize)       AS InstanceSize,
    _date,
    _hour,
    [type],
    businessHour,
    COUNT(DISTINCT ProcessId) AS ProcessCount,

    -- CPU
    AVG(CpuAvg)             AS CpuAvg,
    AVG(CpuAvgP95)          AS CpuAvgP95,
    MAX(CpuMax)             AS CpuMax,
    MAX(CpuMaxP95)          AS CpuMaxP95,
    SUM(CpuMaxGt50)         AS CpuMaxGt50,
    SUM(CpuMaxGt25)         AS CpuMaxGt25,

    -- Memory
    MAX(MemResidentMax)     AS MemResidentMax,
    MAX(MemResidentMaxPct)  AS MemResidentMaxPct,
    AVG(MemResidentAvgPct)  AS MemResidentAvgPct,

    -- Connections
    SUM(ConnectionsMax)     AS ConnectionsMax,
    MAX(ConnUtilizationPct) AS ConnUtilizationPct,

    -- Ops
    MAX(OpcQueryMax)        AS OpcQueryMax

FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ProcessType  = 'REPLICA_PRIMARY'
AND   ClusterName  = 'retail-portal-prod'  -- test one cluster
GROUP BY
    ClusterKey,
    ClusterName,
    _date,
    _hour,
    [type],
    businessHour
ORDER BY _date DESC, _hour DESC
GO

-- BEFORE — per process (current)
SELECT
    ClusterName,
    _date,
    _hour,
    ProcessId,
    CpuMax,
    MemResidentMaxPct,
    ConnectionsMax
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ProcessType = 'REPLICA_PRIMARY'
AND   ClusterName = 'retail-portal-prod'
AND   _date       = CAST(GETDATE() AS DATE)
ORDER BY _hour, ProcessId
GO

-- AFTER — per cluster (aggregated preview)
SELECT
    ClusterName,
    _date,
    _hour,
    COUNT(DISTINCT ProcessId)   AS Processes,
    MAX(CpuMax)                 AS CpuMax,
    MAX(MemResidentMaxPct)      AS MemResidentMaxPct,
    SUM(ConnectionsMax)         AS ConnectionsMax
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ProcessType = 'REPLICA_PRIMARY'
AND   ClusterName = 'retail-portal-prod'
AND   _date       = CAST(GETDATE() AS DATE)
GROUP BY
    ClusterName,
    _date,
    _hour
ORDER BY _hour
GO