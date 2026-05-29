-- Manually verify the math for cdr-uat
SELECT
    ClusterName,
    InstanceSize,
    _date,
    _hour,

    -- Memory validation
    MemResidentMax,
    MemResidentMaxPct,
    -- Expected = (MemResidentMax / (MemorySizeGB * 1024)) * 100
    -- M60 = 64GB = 65,536 MB
    -- (1112 / 65536) * 100 = 1.7% ← should match MemResidentMaxPct

    -- Connection validation
    ConnectionsMax,
    ConnUtilizationPct,
    -- Expected = (ConnectionsMax / 64000) * 100
    -- (2200 / 64000) * 100 = 3.44% ← should match ConnUtilizationPct

    -- P95 validation
    CpuAvg,
    CpuAvgP95,    -- should be >= CpuAvg
    CpuMax,       -- should be >= CpuAvgP95
    CpuMaxP95,    -- should be between CpuAvg and CpuMax

    -- Memory P95 validation
    MemResidentAvgPct,
    MemResidentP95Pct,  -- should be between Avg and Max
    MemResidentMaxPct

FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterKey = 330
AND   _date      = '2026-05-21'
AND   _hour      = 8
GO


-- Compare aggregated value vs raw source for cdr-uat CPU
-- Aggregated value
SELECT
    'Aggregated' AS Source,
    _date,
    _hour,
    CpuMax,
    CpuAvg,
    CpuAvgP95
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterKey = 330
AND   _date = '2026-05-21'
AND   _hour = 8

UNION ALL

-- Raw source value — should match after aggregation
SELECT
    'Raw Source' AS Source,
    CAST(SWITCHOFFSET(CONVERT(datetimeoffset, DateTime),
         '-05:00') AS DATE)                 AS _date,
    DATEPART(HOUR, SWITCHOFFSET(
         CONVERT(datetimeoffset, DateTime),
         '-05:00'))                          AS _hour,
    MAX(Measurement)                         AS CpuMax,
    AVG(Measurement)                         AS CpuAvg,
    NULL                                     AS CpuAvgP95
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M]
WHERE [key] IN (
    SELECT p.ProcessId
    FROM [MongoDB].[Process] p
    JOIN [MongoDB].[Clusters] cl
        ON cl.ClustersKey = p.ClusterKey
    WHERE cl.Name = 'cdr-uat'
)
AND CAST(SWITCHOFFSET(CONVERT(datetimeoffset, DateTime),
    '-05:00') AS DATE) = '2026-05-21'
AND DATEPART(HOUR, SWITCHOFFSET(
    CONVERT(datetimeoffset, DateTime),
    '-05:00')) = 8
GROUP BY
    CAST(SWITCHOFFSET(CONVERT(datetimeoffset, DateTime),
         '-05:00') AS DATE),
    DATEPART(HOUR, SWITCHOFFSET(
         CONVERT(datetimeoffset, DateTime),
         '-05:00'))
GO

-- Find any impossible values
SELECT
    ClusterName,
    InstanceSize,
    _date,
    _hour,

    -- CPU impossible if > 100%
    CASE WHEN CpuMax > 100
         THEN 'CPU > 100% IMPOSSIBLE' END    AS CpuCheck,

    -- P95 impossible if > Max
    CASE WHEN CpuAvgP95 > CpuMax
         THEN 'P95 > Max IMPOSSIBLE' END     AS P95Check,

    -- Memory impossible if > 100%
    CASE WHEN MemResidentMaxPct > 100
         THEN 'Mem > 100% IMPOSSIBLE' END    AS MemCheck,

    -- Avg cannot be > Max
    CASE WHEN MemResidentAvg > MemResidentMax
         THEN 'MemAvg > MemMax IMPOSSIBLE' END AS MemAvgCheck,

    -- Connection % impossible if > 100%
    CASE WHEN ConnUtilizationPct > 100
         THEN 'Conn > 100% IMPOSSIBLE' END   AS ConnCheck

FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE
    CpuMax              > 100
    OR CpuAvgP95        > CpuMax
    OR MemResidentMaxPct > 100
    OR MemResidentAvg   > MemResidentMax
    OR ConnUtilizationPct > 100
GO