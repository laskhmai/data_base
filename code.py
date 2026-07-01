-- Clusters currently NoChange 
-- but CpuMaxP95 is between 25% and 50%
-- These would flip to Downsize if we relax L1 threshold

SELECT
    r.ClusterName,
    r.CurrentSku,
    r.Action,
    ROUND(MAX(a.CpuMaxP95),  2)     AS CpuMaxP95,
    ROUND(MAX(a.CpuAvgP95),  2)     AS CpuAvgP95,
    ROUND(MAX(a.CpuMaxP95)*2,2)     AS CpuMaxP95x2,
    ROUND(MAX(a.CpuAvg),     2)     AS AvgCpu,
    ROUND(MAX(a.CpuMax),     2)     AS PeakCpu
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = r.ClusterKey
    AND a.[type]       = r.DayType
    AND a.businessHour = r.HourType
    AND FORMAT(a._date,'yyyy-MM') = r.Month
WHERE r.Month      = '2026-06'
AND   r.DayType    = 'Weekday'
AND   r.HourType   = 'BusinessHours'
AND   r.Action     = 'NoChange'
GROUP BY
    r.ClusterName,
    r.CurrentSku,
    r.Action
HAVING
    MAX(a.CpuMaxP95) > 25    -- above current L1 threshold
AND MAX(a.CpuMaxP95) < 50    -- below Level1 safety check limit
AND MAX(a.CpuMaxP95) * 2 < 100  -- would pass Level1 safety check
ORDER BY CpuMaxP95 DESC
GO

-- Count summary
SELECT
    COUNT(DISTINCT r.ClusterKey)    AS ClustersAffected
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = r.ClusterKey
    AND a.[type]       = r.DayType
    AND a.businessHour = r.HourType
    AND FORMAT(a._date,'yyyy-MM') = r.Month
WHERE r.Month      = '2026-06'
AND   r.DayType    = 'Weekday'
AND   r.HourType   = 'BusinessHours'
AND   r.Action     = 'NoChange'
GROUP BY r.ClusterKey
HAVING
    MAX(a.CpuMaxP95) > 25
AND MAX(a.CpuMaxP95) < 50
AND MAX(a.CpuMaxP95) * 2 < 100
GO