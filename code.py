
-- 1A: Basic Stats of Aggregation Table
SELECT
    COUNT(*)                        AS TotalRows,
    COUNT(DISTINCT ClusterKey)      AS TotalClusters,
    MIN(_date)                      AS DataFrom,
    MAX(_date)                      AS DataTo
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GO

-- 1B: Sample Data — One Cluster One Day
SELECT TOP 24
    ClusterName,
    InstanceSize,
    _date,
    _hour,
    [type]          AS DayType,
    businessHour    AS HourType,
    ROUND(CpuAvg,             2) AS CpuAvg,
    ROUND(CpuMax,             2) AS CpuMax,
    ROUND(CpuMaxP95,          2) AS CpuP95,
    CpuMaxGt50,
    ROUND(MemResidentAvgPct,  2) AS MemAvgPct,
    ROUND(MemResidentMaxPct,  2) AS MemMaxPct,
    ROUND(ConnUtilizationPct, 2) AS ConnPct
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'cdr-uat'           -- change cluster name
AND   _date       = '2026-05-22'        -- change date
ORDER BY _hour
GO


-- ============================================================
-- SECTION 2: RECOMMENDATIONS OVERVIEW
-- ============================================================

-- 2A: Action Breakdown
SELECT
    Action,
    COUNT(DISTINCT ClusterKey)              AS UniqueClusters,
    COUNT(*)                                AS TotalRows,
    ROUND(SUM(EstimatedMonthlySavings), 0)  AS TotalMonthlySavings
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE DayType  = 'Weekday'
AND   HourType = 'BusinessHours'
GROUP BY Action
ORDER BY TotalMonthlySavings DESC
GO

-- 2B: Sample Recommendations
SELECT TOP 20
    ClusterName,
    CurrentSku,
    RecommendedSku,
    Action,
    ROUND(AvgCpuMax,          2) AS AvgCpu,
    ROUND(PeakCpuMax,         2) AS PeakCpu,
    ROUND(MemUtilizationPct,  2) AS MemPct,
    ROUND(ConnUtilizationPct, 2) AS ConnPct,
    ROUND(EstimatedMonthlySavings, 2) AS MonthlySavings,
    Comment
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE DayType  = 'Weekday'
AND   HourType = 'BusinessHours'
ORDER BY Action, ClusterName
GO


-- ============================================================
-- SECTION 3: MANUAL CPU EVALUATION
-- Replace cluster names as needed
-- ============================================================

-- 3A: CPU Summary From Aggregation Table
SELECT
    a.ClusterName,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action,
    ROUND(AVG(a.CpuAvg),    2) AS OverallAvgCpu,
    ROUND(MAX(a.CpuMax),    2) AS OverallMaxCpu,
    ROUND(AVG(a.CpuMaxP95), 2) AS OverallP95Cpu,
    SUM(a.CpuMaxGt50)          AS TotalHoursAbove50Pct,
    SUM(a.CpuMaxGt25)          AS TotalHoursAbove25Pct,
    COUNT(*)                   AS TotalHours
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
JOIN [Metrics].[MongoDBRightsizingRecommendations] r
    ON  r.ClusterKey = a.ClusterKey
    AND r.DayType    = 'Weekday'
    AND r.HourType   = 'BusinessHours'
WHERE a.[type]       = 'Weekday'
AND   a.businessHour = 'BusinessHours'
AND   a.ClusterName IN (
    'cdr-uat',
    'cwih-cp-mgmt-prod',
    'consumer-interops-uat'
    -- add more clusters here
)
GROUP BY
    a.ClusterName,
    r.CurrentSku, r.RecommendedSku, r.Action
ORDER BY OverallAvgCpu DESC
GO

-- 3B: CPU From Raw Source Table (deeper verification)
SELECT
    cl.Name                             AS ClusterName,
    p.ProcessType,
    CAST(m.DateTime AS DATE)            AS Date,
    DATEPART(HOUR, m.DateTime)          AS Hour,
    ROUND(AVG(m.Measurement), 2)        AS RawCpuAvg,
    ROUND(MAX(m.Measurement), 2)        AS RawCpuMax,
    COUNT(*)                            AS Readings
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_5M] m
JOIN [MongoDB].[Process] p
    ON  p.ProcessId  = m.[key]
    AND p.IsDeleted  = 0
JOIN [MongoDB].[Clusters] cl
    ON  cl.ClustersKey = p.ClusterKey
WHERE cl.Name = 'cdr-uat'              -- change cluster name
AND   m.DateTime >= DATEADD(DAY, -20, GETDATE())
GROUP BY
    cl.Name,
    p.ProcessType,
    CAST(m.DateTime AS DATE),
    DATEPART(HOUR, m.DateTime)
ORDER BY Date, Hour, ProcessType
GO


-- ============================================================
-- SECTION 4: MANUAL MEMORY EVALUATION
-- ============================================================

-- 4A: Memory Summary From Aggregation Table
SELECT
    a.ClusterName,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action,
    ROUND(AVG(a.MemResidentAvgPct), 2) AS OverallAvgMemPct,
    ROUND(MAX(a.MemResidentMaxPct), 2) AS OverallMaxMemPct,
    ROUND(AVG(a.MemResidentP95Pct), 2) AS OverallP95MemPct,
    COUNT(*)                           AS TotalHours
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
JOIN [Metrics].[MongoDBRightsizingRecommendations] r
    ON  r.ClusterKey = a.ClusterKey
    AND r.DayType    = 'Weekday'
    AND r.HourType   = 'BusinessHours'
WHERE a.[type]       = 'Weekday'
AND   a.businessHour = 'BusinessHours'
AND   a.ClusterName IN (
    'cdr-uat',
    'cwih-cp-mgmt-prod',
    'consumer-interops-uat'
    -- add more clusters here
)
GROUP BY
    a.ClusterName,
    r.CurrentSku, r.RecommendedSku, r.Action
ORDER BY OverallAvgMemPct DESC
GO

-- 4B: Memory From Raw Source Table
SELECT
    cl.Name                             AS ClusterName,
    p.ProcessType,
    CAST(m.DateTime AS DATE)            AS Date,
    DATEPART(HOUR, m.DateTime)          AS Hour,
    ROUND(AVG(m.Measurement), 2)        AS RawMemAvgMB,
    ROUND(MAX(m.Measurement), 2)        AS RawMemMaxMB,
    COUNT(*)                            AS Readings
FROM [Metrics].[MongoDB_Memory_Resident_5M] m
JOIN [MongoDB].[Process] p
    ON  p.ProcessId  = m.[key]
    AND p.IsDeleted  = 0
JOIN [MongoDB].[Clusters] cl
    ON  cl.ClustersKey = p.ClusterKey
WHERE cl.Name = 'cdr-uat'              -- change cluster name
AND   m.DateTime >= DATEADD(DAY, -20, GETDATE())
GROUP BY
    cl.Name,
    p.ProcessType,
    CAST(m.DateTime AS DATE),
    DATEPART(HOUR, m.DateTime)
ORDER BY Date, Hour, ProcessType
GO


-- ============================================================
-- SECTION 5: RAW vs AGGREGATED SIDE BY SIDE
-- Proves our proc is calculating correctly
-- ============================================================
SELECT
    a.ClusterName,
    a._date,
    a._hour,
    -- From aggregation table
    ROUND(a.CpuAvg,            2) AS Agg_CpuAvg,
    ROUND(a.CpuMax,            2) AS Agg_CpuMax,
    ROUND(a.MemResidentMaxPct, 2) AS Agg_MemMaxPct,
    ROUND(a.ConnUtilizationPct,2) AS Agg_ConnPct,
    -- From raw source table
    ROUND(AVG(rc.Measurement), 2) AS Raw_CpuAvg,
    ROUND(MAX(rc.Measurement), 2) AS Raw_CpuMax
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
JOIN [MongoDB].[Clusters] cl
    ON  cl.Name        = a.ClusterName
JOIN [MongoDB].[Process] p
    ON  p.ClusterKey   = cl.ClustersKey
    AND p.IsDeleted    = 0
JOIN [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_5M] rc
    ON  rc.[key]                    = p.ProcessId
    AND CAST(rc.DateTime AS DATE)   = a._date
    AND DATEPART(HOUR, rc.DateTime) = a._hour
WHERE a.ClusterName   = 'cdr-uat'      -- change cluster name
AND   a.[type]        = 'Weekday'
AND   a.businessHour  = 'BusinessHours'
GROUP BY
    a.ClusterName, a._date, a._hour,
    a.CpuAvg, a.CpuMax,
    a.MemResidentMaxPct, a.ConnUtilizationPct
ORDER BY a._date, a._hour
GO


-- ============================================================
-- SECTION 6: SPECIFIC CLUSTER FULL DETAIL
-- One stop query for any cluster
-- ============================================================
DECLARE @Cluster NVARCHAR(255) = 'cdr-uat'  -- change here

-- Part A: Aggregation summary
SELECT
    a.ClusterName,
    a.InstanceSize,
    a.ProviderName,
    a.RegionName,
    COUNT(*)                              AS TotalHours,
    ROUND(AVG(a.CpuAvg),            2)   AS AvgCpu,
    ROUND(MAX(a.CpuMax),            2)   AS MaxCpu,
    ROUND(AVG(a.MemResidentAvgPct), 2)   AS AvgMemPct,
    ROUND(MAX(a.MemResidentMaxPct), 2)   AS MaxMemPct,
    ROUND(AVG(a.ConnUtilizationPct),2)   AS AvgConnPct,
    ROUND(MAX(a.ConnUtilizationPct),2)   AS MaxConnPct
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
WHERE a.ClusterName = @Cluster
GROUP BY a.ClusterName, a.InstanceSize, a.ProviderName, a.RegionName
GO

-- Part B: Recommendations for this cluster
SELECT
    r.ClusterName,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action,
    r.DayType,
    r.HourType,
    r.Comment,
    ROUND(r.AvgCpuMax,          2) AS AvgCpu,
    ROUND(r.PeakCpuMax,         2) AS PeakCpu,
    ROUND(r.MemUtilizationPct,  2) AS MemPct,
    ROUND(r.ConnUtilizationPct, 2) AS ConnPct,
    ROUND(r.EstimatedMonthlySavings, 2) AS MonthlySavings,
    ROUND(r.Spend30days,        2) AS CurrentSpend
FROM [Metrics].[MongoDBRightsizingRecommendations] r
WHERE r.ClusterName = 'cdr-uat'   -- change cluster name
ORDER BY r.DayType, r.HourType
GO
