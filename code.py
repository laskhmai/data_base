-- Pick 2 Downsize, 2 NoChange, 2 Upsize
SELECT TOP 6
    ClusterName,
    CurrentSku,
    RecommendedSku,
    Action,
    AvgCpuMax,
    PeakCpuMax,
    MemUtilizationPct,
    ConnUtilizationPct
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE DayType  = 'Weekday'
AND   HourType = 'BusinessHours'
ORDER BY Action, ClusterName
GO

-- Replace 'cdr-uat' with your cluster name
SELECT
    ClusterName,
    _date,
    _hour,
    ROUND(CpuAvg,    2) AS CpuAvg,
    ROUND(CpuMax,    2) AS CpuMax,
    ROUND(CpuMaxP95, 2) AS CpuMaxP95,
    CpuMaxGt50,
    CpuMaxGt25,
    [type],
    businessHour
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'cdr-uat'
AND   [type]      = 'Weekday'
AND   businessHour = 'BusinessHours'
ORDER BY _date, _hour
GO

-- Quick summary to verify recommendation
SELECT
    ClusterName,
    ROUND(AVG(CpuAvg),        2) AS OverallAvgCpu,
    ROUND(MAX(CpuMax),        2) AS OverallMaxCpu,
    ROUND(AVG(CpuMaxP95),     2) AS OverallP95Cpu,
    SUM(CpuMaxGt50)              AS TotalHoursAbove50,
    SUM(CpuMaxGt25)              AS TotalHoursAbove25,
    COUNT(*)                     AS TotalHours,
    -- Recommendation it got
    MAX(r.Action)                AS Recommendation,
    MAX(r.RecommendedSku)        AS RecommendedSku
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
JOIN [Metrics].[MongoDBRightsizingRecommendations] r
    ON  r.ClusterKey = a.ClusterKey
    AND r.DayType    = 'Weekday'
    AND r.HourType   = 'BusinessHours'
WHERE a.[type]       = 'Weekday'
AND   a.businessHour = 'BusinessHours'
AND   a.ClusterName IN (
    'cdr-uat',
    'consumer-interops-uat',
    'cwih-cp-mgmt-prod'
    -- add more cluster names here
)
GROUP BY a.ClusterName
ORDER BY OverallAvgCpu DESC
GO