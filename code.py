-- Find clusters where CPU is low but connections are high
-- These are clusters that CANNOT be downsized
-- because connections would break

SELECT
    r.ClusterName,
    r.CurrentSku,
    r.DayType,
    r.HourType,
    r.Action,
    r.CpuRec,
    r.ConnRec,
    ROUND(r.PeakCpuMax,        2)  AS PeakCpuMax,
    ROUND(MAX(a.CpuMaxP95),    2)  AS CpuMaxP95,
    ROUND(MAX(a.CpuMaxP95)*2,  2)  AS CpuMaxP95x2,
    ROUND(MAX(a.ConnUtilizationPct), 2) AS MaxConnUtil,
    r.Comment
FROM [Metrics].[MongoDBRightsizingRecommendations_STL] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = r.ClusterKey
    AND a.[type]       = r.DayType
    AND a.businessHour = r.HourType
    AND FORMAT(a._date,'yyyy-MM') = r.Month
WHERE r.Month = '2026-06'
AND   r.DayType = 'Weekday'
AND   r.HourType = 'BusinessHours'
GROUP BY
    r.ClusterName, r.CurrentSku,
    r.DayType, r.HourType, r.Action,
    r.CpuRec, r.ConnRec,
    r.PeakCpuMax, r.Comment
HAVING
    MAX(a.CpuMaxP95) * 2 < 100   -- CPU safe to downsize
AND MAX(a.ConnUtilizationPct) > 80 -- BUT connections high!
ORDER BY MaxConnUtil DESC
GO

-- Summary count
SELECT
    COUNT(DISTINCT r.ClusterKey)    AS ClustersAffected,
    COUNT(*)                        AS TotalSlices
FROM [Metrics].[MongoDBRightsizingRecommendations_STL] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = r.ClusterKey
    AND a.[type]       = r.DayType
    AND a.businessHour = r.HourType
    AND FORMAT(a._date,'yyyy-MM') = r.Month
WHERE r.Month = '2026-06'
GROUP BY r.ClusterKey, r.DayType, r.HourType
HAVING
    MAX(a.CpuMaxP95) * 2 < 100
AND MAX(a.ConnUtilizationPct) > 80
GO