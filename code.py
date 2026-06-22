-- =============================================
-- RAW CHECK 1: Validate CpuAvg for cdr-uat
-- Compare aggregated vs raw metrics table
-- Uses MaxCpuProcessId to trace back
-- =============================================
DECLARE @ClusterName NVARCHAR(255) = 'cdr-uat'
DECLARE @Date        DATE          = '2026-06-16'
DECLARE @Hour        INT           = 0

-- What aggregated table says
SELECT
    'Aggregated'            AS Source,
    a.ClusterName,
    a._date                 AS [Date],
    a._hour                 AS [Hour],
    a.[type]                AS DayType,
    a.businessHour          AS HourType,
    ROUND(a.CpuAvg, 2)     AS CpuAvg,
    ROUND(a.CpuMax, 2)     AS CpuMax,
    ROUND(a.CpuAvgP95, 2)  AS CpuAvgP95,
    a.MaxCpuProcessId       AS ProcessId
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
WHERE a.ClusterName = @ClusterName
AND   a._date       = @Date
AND   a._hour       = @Hour
GO

-- =============================================
-- RAW CHECK 2: Validate against raw process table
-- Get the actual process metrics for same hour
-- =============================================
DECLARE @ClusterName NVARCHAR(255) = 'cdr-uat'
DECLARE @Date        DATE          = '2026-06-16'
DECLARE @Hour        INT           = 0

SELECT
    'Raw'                              AS Source,
    p.ClusterName,
    CAST(m.DateTime AS DATE)           AS [Date],
    DATEPART(HOUR, m.DateTime)         AS [Hour],
    ROUND(AVG(m.CpuAvg), 2)           AS CpuAvg,
    ROUND(MAX(m.CpuMax), 2)           AS CpuMax,
    COUNT(DISTINCT m.ProcessId)        AS ProcessCount
FROM [MongoDB].[Metrics] m
JOIN [MongoDB].[Process] p
    ON p.ProcessId = m.ProcessId
WHERE p.ClusterName     = @ClusterName
AND CAST(m.DateTime AS DATE) = @Date
AND DATEPART(HOUR, m.DateTime) = @Hour
GROUP BY p.ClusterName,
         CAST(m.DateTime AS DATE),
         DATEPART(HOUR, m.DateTime)
GO

-- =============================================
-- RAW CHECK 3: Validate Memory for cdr-uat
-- =============================================
DECLARE @ClusterName NVARCHAR(255) = 'cdr-uat'
DECLARE @Date        DATE          = '2026-06-16'

SELECT
    'Aggregated Memory'                AS Source,
    a.ClusterName,
    a._date,
    a._hour,
    ROUND(a.MemResidentAvgPct, 2)     AS MemAvgPct,
    ROUND(a.MemResidentMaxPct, 2)     AS MemMaxPct,
    a.MaxMemProcessId                  AS ProcessId
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
WHERE a.ClusterName = @ClusterName
AND   a._date       = @Date
ORDER BY a._hour
GO

-- =============================================
-- RAW CHECK 4: Validate Connections for
-- consumer-interops-uat (high connections)
-- =============================================
SELECT
    'Aggregated'                AS Source,
    a.ClusterName,
    a._date,
    a._hour,
    a.businessHour,
    ROUND(a.ConnUtilizationPct, 2) AS ConnUtilizationPct,
    a.ConnectionsMax,
    a.Action
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
LEFT JOIN [Metrics].[MongoDBRightsizingRecommendations] r
    ON r.ClusterKey = a.ClusterKey
    AND r.DayType   = a.[type]
    AND r.HourType  = a.businessHour
WHERE a.ClusterName = 'consumer-interops-uat'
AND   a._date      >= DATEADD(DAY, -3, CAST(GETDATE() AS DATE))
ORDER BY a._date DESC, a._hour
GO

-- =============================================
-- RAW CHECK 5: Verify Simulated Math is correct
-- CpuProjected = CpuAvg × (currentVCores/recVCores)
-- cdr-uat: M60(16vCores) → M50(8vCores)
-- Expected: CpuAvg × 2 = ProjectedCpu
-- =============================================
SELECT TOP 10
    s.ClusterName,
    s.[Date],
    s.[Hour],
    ROUND(s.CpuAvg, 2)              AS CpuAvg_Current,
    ROUND(s.nCpuAvgWithin, 2)       AS CpuAvg_Projected,
    ROUND(s.nCpuAvgWithin
         / NULLIF(s.CpuAvg, 0), 2)  AS Ratio,
    '2.0 expected (16/8)'           AS ExpectedRatio
FROM [Metrics].[MongoDBRightsizingSimulatedMetrics] s
WHERE s.ClusterName = 'cdr-uat'
AND   s.CpuAvg     > 0
ORDER BY s.[Date] DESC, s.[Hour]
GO