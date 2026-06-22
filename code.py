-- =============================================
-- RAW CHECK 1: Validate CPU for cdr-uat
-- =============================================
DECLARE @ClusterKey INT = (
    SELECT TOP 1 ClusterKey
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE ClusterName = 'cdr-uat'
)
DECLARE @Date DATE = '2026-06-16'
DECLARE @Hour INT  = 0

-- Aggregated
SELECT
    'Aggregated'                    AS Source,
    a.ClusterName,
    a._date                         AS [Date],
    a._hour                         AS [Hour],
    ROUND(a.CpuAvg,    2)          AS CpuAvg,
    ROUND(a.CpuMax,    2)          AS CpuMax,
    ROUND(a.CpuAvgP95, 2)          AS CpuAvgP95,
    a.MaxCpuProcessId               AS ProcessId
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
WHERE a.ClusterKey = @ClusterKey
AND   a._date      = @Date
AND   a._hour      = @Hour

UNION ALL

-- Raw CPU (Measurement column)
SELECT
    'Raw CPU Table'                 AS Source,
    'cdr-uat'                       AS ClusterName,
    CAST(c.DateTime AS DATE)        AS [Date],
    DATEPART(HOUR, c.DateTime)      AS [Hour],
    ROUND(AVG(c.Measurement), 2)    AS CpuAvg,
    ROUND(MAX(c.Measurement), 2)    AS CpuMax,
    NULL                            AS CpuAvgP95,
    c.[Key]                         AS ProcessId
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M] c
JOIN [MongoDB].[Process] p
    ON  p.ProcessId  = c.[Key]
    AND p.ClusterKey = @ClusterKey
    AND p.IsDeleted  = 0
WHERE CAST(c.DateTime AS DATE)   = @Date
AND   DATEPART(HOUR, c.DateTime) = @Hour
GROUP BY
    CAST(c.DateTime AS DATE),
    DATEPART(HOUR, c.DateTime),
    c.[Key]
ORDER BY Source
GO

-- =============================================
-- RAW CHECK 2: Validate Memory for cdr-uat
-- =============================================
DECLARE @ClusterKey INT = (
    SELECT TOP 1 ClusterKey
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE ClusterName = 'cdr-uat'
)
DECLARE @Date DATE = '2026-06-16'
DECLARE @Hour INT  = 0

SELECT
    'Aggregated Memory'             AS Source,
    a.ClusterName,
    a._date,
    a._hour,
    ROUND(a.MemResidentAvgPct, 2)  AS MemAvgPct,
    ROUND(a.MemResidentMaxPct, 2)  AS MemMaxPct,
    a.MaxMemProcessId               AS ProcessId
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
WHERE a.ClusterKey = @ClusterKey
AND   a._date      = @Date
AND   a._hour      = @Hour

UNION ALL

SELECT
    'Raw Memory Table'              AS Source,
    'cdr-uat'                       AS ClusterName,
    CAST(m.DateTime AS DATE),
    DATEPART(HOUR, m.DateTime),
    ROUND(AVG(m.Measurement), 2)    AS MemAvgPct,
    ROUND(MAX(m.Measurement), 2)    AS MemMaxPct,
    m.[Key]                         AS ProcessId
FROM [Metrics].[MongoDB_Memory_Resident_5M] m
JOIN [MongoDB].[Process] p
    ON  p.ProcessId  = m.[Key]
    AND p.ClusterKey = @ClusterKey
    AND p.IsDeleted  = 0
WHERE CAST(m.DateTime AS DATE)   = @Date
AND   DATEPART(HOUR, m.DateTime) = @Hour
GROUP BY
    CAST(m.DateTime AS DATE),
    DATEPART(HOUR, m.DateTime),
    m.[Key]
ORDER BY Source
GO

-- =============================================
-- RAW CHECK 3: Validate Connections
-- consumer-interops-uat
-- =============================================
DECLARE @ClusterKey INT = (
    SELECT TOP 1 ClusterKey
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE ClusterName = 'consumer-interops-uat'
)
DECLARE @Date DATE = '2026-06-16'
DECLARE @Hour INT  = 9

SELECT
    'Aggregated'                    AS Source,
    a.ClusterName,
    a._date,
    a._hour,
    ROUND(a.ConnUtilizationPct, 2) AS ConnUtilizationPct,
    a.ConnectionsMax,
    a.MaxCpuProcessId               AS ProcessId
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
WHERE a.ClusterKey = @ClusterKey
AND   a._date      = @Date
AND   a._hour      = @Hour

UNION ALL

SELECT
    'Raw Connections Table'         AS Source,
    'consumer-interops-uat',
    CAST(c.DateTime AS DATE),
    DATEPART(HOUR, c.DateTime),
    NULL                            AS ConnUtilizationPct,
    ROUND(MAX(c.Measurement), 0)   AS ConnectionsMax,
    c.[Key]                         AS ProcessId
FROM [Metrics].[MongoDB_Connections_15M] c
JOIN [MongoDB].[Process] p
    ON  p.ProcessId  = c.[Key]
    AND p.ClusterKey = @ClusterKey
    AND p.IsDeleted  = 0
WHERE CAST(c.DateTime AS DATE)   = @Date
AND   DATEPART(HOUR, c.DateTime) = @Hour
GROUP BY
    CAST(c.DateTime AS DATE),
    DATEPART(HOUR, c.DateTime),
    c.[Key]
ORDER BY Source
GO

-- =============================================
-- RAW CHECK 4: Simulated Math Validation
-- cdr-uat M60→M50
-- CPU ratio = 2.0 (16/8)
-- Mem ratio = 2.0 (64GB/32GB)
-- =============================================
SELECT TOP 10
    s.ClusterName,
    s.[Date],
    s.[Hour],
    s.CurrentSku,
    ROUND(s.CpuAvg,        2)      AS CpuAvg_Current,
    ROUND(s.nCpuAvgWithin, 2)      AS CpuAvg_Projected,
    ROUND(s.nCpuAvgWithin
        / NULLIF(s.CpuAvg, 0), 2)  AS CpuRatio,
    ROUND(s.MemAvg,        2)      AS MemAvg_Current,
    ROUND(s.nMemAvgWithin, 2)      AS MemAvg_Projected,
    ROUND(s.nMemAvgWithin
        / NULLIF(s.MemAvg, 0), 2)  AS MemRatio,
    CASE
        WHEN ABS(s.nCpuAvgWithin
            / NULLIF(s.CpuAvg,0) - 2.0) < 0.01
        AND  ABS(s.nMemAvgWithin
            / NULLIF(s.MemAvg,0) - 2.0) < 0.01
        THEN 'Pass'
        ELSE 'Fail'
    END                            AS ValidationStatus
FROM [Metrics].[MongoDBRightsizingSimulatedMetrics] s
WHERE s.ClusterName = 'cdr-uat'
AND   s.CpuAvg     > 0
AND   s.MemAvg     > 0
ORDER BY s.[Date] DESC, s.[Hour]
GO