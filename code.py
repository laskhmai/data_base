-- How many current Upsize clusters
-- have low CpuAvgP95?
SELECT
    r.ClusterName,
    r.DayType,
    r.HourType,
    r.Action,
    ROUND(MAX(a.CpuMaxP95), 2)   AS CpuMaxP95,
    ROUND(MAX(a.CpuAvgP95), 2)   AS CpuAvgP95,
    ROUND(MAX(a.CpuMax),    2)   AS PeakCpuMax
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = r.ClusterKey
    AND a.[type]       = r.DayType
    AND a.businessHour = r.HourType
WHERE r.Action = 'Upsize'
AND   r.Month  = '2026-06'
GROUP BY
    r.ClusterName, r.DayType,
    r.HourType, r.Action
ORDER BY CpuAvgP95 ASC
GO


-- How many STL Downsize clusters
-- have CpuMaxP95×2 > 100%?
SELECT
    s.ClusterName,
    s.Action,
    ROUND(MAX(a.CpuMaxP95),  2)  AS CpuMaxP95,
    ROUND(MAX(a.CpuMaxP95)*2,2)  AS CpuMaxP95x2
FROM [Metrics].[MongoDBRightsizingRecommendations_STL] s
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = s.ClusterKey
    AND a.[type]       = s.DayType
    AND a.businessHour = s.HourType
WHERE s.Action = 'Downsize'
AND   s.Month  = '2026-06'
GROUP BY s.ClusterName, s.Action
HAVING MAX(a.CpuMaxP95) * 2 > 100
ORDER BY CpuMaxP95x2 DESC
GO




-- How many Upsize clusters have
-- CpuAvgP95 < 20%?
SELECT
    r.ClusterName,
    r.DayType,
    r.HourType,
    r.Action,
    ROUND(MAX(a.CpuAvgP95), 2)  AS CpuAvgP95,
    ROUND(MAX(a.CpuMaxP95), 2)  AS CpuMaxP95
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = r.ClusterKey
    AND a.[type]       = r.DayType
    AND a.businessHour = r.HourType
WHERE r.Action = 'Upsize'
AND   r.Month  = '2026-06'
GROUP BY
    r.ClusterName, r.DayType,
    r.HourType, r.Action
HAVING MAX(a.CpuAvgP95) < 20
ORDER BY CpuAvgP95 ASC
GO