-- =============================================
-- RAW CHECK 1: Validate CPU for cdr-uat
-- Compare aggregated vs raw CPU table
-- =============================================
DECLARE @ClusterName NVARCHAR(255) = 'cdr-uat'
DECLARE @Date        DATE          = '2026-06-16'
DECLARE @Hour        INT           = 0

-- Aggregated value
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
WHERE a.ClusterName = @ClusterName
AND   a._date       = @Date
AND   a._hour       = @Hour

UNION ALL

-- Raw CPU value
SELECT
    'Raw CPU Table'                 AS Source,
    p.ClusterName,
    CAST(c.[DateTime] AS DATE)      AS [Date],
    DATEPART(HOUR, c.[DateTime])    AS [Hour],
    ROUND(AVG(c.[Value]), 2)        AS CpuAvg,
    ROUND(MAX(c.[Value]), 2)        AS CpuMax,
    NULL                            AS CpuAvgP95,
    c.[Key]                         AS ProcessId
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M] c
JOIN [MongoDB].[Process] p
    ON p.ProcessId = c.[Key]
WHERE p.ClusterName              = @ClusterName
AND   CAST(c.[DateTime] AS DATE) = @Date
AND   DATEPART(HOUR, c.[DateTime]) = @Hour
GROUP BY
    p.ClusterName,
    CAST(c.[DateTime] AS DATE),
    DATEPART(HOUR, c.[DateTime]),
    c.[Key]
ORDER BY Source
GO

-- =============================================
-- RAW CHECK 2: Validate Memory for cdr-uat
-- =============================================
DECLARE @ClusterName NVARCHAR(255) = 'cdr-uat'
DECLARE @Date        DATE          = '2026-06-16'
DECLARE @Hour        INT           = 0

SELECT
    'Aggregated Memory'             AS Source,
    a.ClusterName,
    a._date,
    a._hour,
    ROUND(a.MemResidentAvgPct, 2)  AS MemAvgPct,
    ROUND(a.MemResidentMaxPct, 2)  AS MemMaxPct,
    a.MaxMemProcessId               AS ProcessId
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
WHERE a.ClusterName = @ClusterName
AND   a._date       = @Date
AND   a._hour       = @Hour

UNION ALL

SELECT
    'Raw Memory Table'              AS Source,
    p.ClusterName,
    CAST(m.[DateTime] AS DATE),
    DATEPART(HOUR, m.[DateTime]),
    ROUND(AVG(m.[Value]), 2)        AS MemAvgPct,
    ROUND(MAX(m.[Value]), 2)        AS MemMaxPct,
    m.[Key]                         AS ProcessId
FROM [Metrics].[MongoDB_Memory_Resident_5M] m
JOIN [MongoDB].[Process] p
    ON p.ProcessId = m.[Key]
WHERE p.ClusterName                  = @ClusterName
AND   CAST(m.[DateTime] AS DATE)     = @Date
AND   DATEPART(HOUR, m.[DateTime])   = @Hour
GROUP BY
    p.ClusterName,
    CAST(m.[DateTime] AS DATE),
    DATEPART(HOUR, m.[DateTime]),
    m.[Key]
ORDER BY Source
GO

-- =============================================
-- RAW CHECK 3: Validate Connections
-- consumer-interops-uat high connections
-- =============================================
DECLARE @ClusterName NVARCHAR(255) = 'consumer-interops-uat'
DECLARE @Date        DATE          = '2026-06-16'
DECLARE @Hour        INT           = 9

SELECT
    'Aggregated'                    AS Source,
    a.ClusterName,
    a._date,
    a._hour,
    ROUND(a.ConnUtilizationPct, 2) AS ConnUtilizationPct,
    a.ConnectionsMax
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
WHERE a.ClusterName = @ClusterName
AND   a._date       = @Date
AND   a._hour       = @Hour

UNION ALL

SELECT
    'Raw Connections Table'         AS Source,
    p.ClusterName,
    CAST(c.[DateTime] AS DATE),
    DATEPART(HOUR, c.[DateTime]),
    NULL                            AS ConnUtilizationPct,
    ROUND(MAX(c.[Value]), 0)        AS ConnectionsMax
FROM [Metrics].[MongoDB_Connections_15M] c
JOIN [MongoDB].[Process] p
    ON p.ProcessId = c.[Key]
WHERE p.ClusterName                  = @ClusterName
AND   CAST(c.[DateTime] AS DATE)     = @Date
AND   DATEPART(HOUR, c.[DateTime])   = @Hour
GROUP BY
    p.ClusterName,
    CAST(c.[DateTime] AS DATE),
    DATEPART(HOUR, c.[DateTime])
ORDER BY Source
GO

-- =============================================
-- RAW CHECK 4: Simulated Math Validation
-- cdr-uat: M60(16 vCores) → M50(8 vCores)
-- Ratio must = 2.0 for all rows
-- =============================================
SELECT TOP 10
    s.ClusterName,
    s.[Date],
    s.[Hour],
    s.DayType,
    s.CurrentSku,
    ROUND(s.CpuAvg,         2)     AS CpuAvg_Current,
    ROUND(s.nCpuAvgWithin,  2)     AS CpuAvg_Projected,
    ROUND(s.nCpuAvgWithin
        / NULLIF(s.CpuAvg, 0), 2)  AS Ratio,
    '2.0 expected (16/8)'          AS ExpectedRatio,
    CASE
        WHEN ABS(s.nCpuAvgWithin
            / NULLIF(s.CpuAvg,0) - 2.0) < 0.01
        THEN '✅ Correct'
        ELSE '❌ Wrong'
    END                            AS RatioCheck
FROM [Metrics].[MongoDBRightsizingSimulatedMetrics] s
WHERE s.ClusterName = 'cdr-uat'
AND   s.CpuAvg     > 0
ORDER BY s.[Date] DESC, s.[Hour]
GO