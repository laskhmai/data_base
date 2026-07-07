-- Deep dive: 2 wrong + 4 edge cases
-- Check raw metrics to understand what is happening

DECLARE @Month CHAR(7) = '2026-06'

SELECT
    n.ClusterName,
    n.DayType,
    n.HourType,
    n.Action                            AS Normal_Action,
    s.Action                            AS STL_Action,
    ROUND(MAX(a.CpuMaxP95),  2)         AS CpuMaxP95,
    ROUND(MAX(a.CpuMaxP95)*2,2)         AS CpuMaxP95x2,
    ROUND(MAX(a.CpuAvgP95),  2)         AS CpuAvgP95,
    ROUND(MAX(a.CpuMax),     2)         AS PeakCpuMax,
    ROUND(AVG(a.CpuAvg),     2)         AS AvgCpuAvg,
    ROUND(MAX(a.ConnUtilizationPct),2)  AS MaxConnUtil,
    -- How many hours had high CPU?
    SUM(CASE WHEN a.CpuMax > 80
             THEN 1 ELSE 0 END)         AS HoursAbove80,
    SUM(CASE WHEN a.CpuMax > 50
             THEN 1 ELSE 0 END)         AS HoursAbove50,
    COUNT(*)                            AS TotalHours
FROM [Metrics].[MongoDBRightsizingRecommendations] n
JOIN [Metrics].[MongoDBRightsizingRecommendations_STL] s
    ON  s.ClusterKey = n.ClusterKey
    AND s.DayType    = n.DayType
    AND s.HourType   = n.HourType
    AND s.Month      = n.Month
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = n.ClusterKey
    AND a.[type]       = n.DayType
    AND a.businessHour = n.HourType
    AND FORMAT(a._date,'yyyy-MM') = @Month
WHERE n.ClusterName IN (
    -- 2 wrong STL clusters
    'cwh-cp-mgmt-dev',
    'ecr-cld3-qa',
    -- 4 edge cases
    'HCaaS-PRD-ClaimIngestor',
    'HCaaS-PRD-Member',
    'Cluster0',
    'ma-dep-prod'
)
AND n.Action != s.Action
GROUP BY
    n.ClusterName, n.DayType, n.HourType,
    n.Action, s.Action
ORDER BY n.ClusterName
GO