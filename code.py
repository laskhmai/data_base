-- Find clusters where recommendation differs
-- between STL table and Standard table
SELECT
    s.ClusterName,
    s.DayType,
    s.HourType,
    s.Action                            AS STL_Action,
    n.Action                            AS Normal_Action,
    s.RecommendedSku                    AS STL_RecommendedSku,
    n.RecommendedSku                    AS Normal_RecommendedSku,
    ROUND(n.AvgCpuMax,   2)            AS AvgCpuMax,
    ROUND(n.PeakCpuMax,  2)            AS PeakCpuMax,
    ROUND(n.ConnUtilizationPct, 2)     AS ConnUtil,
    n.Comment                           AS Normal_Comment,
    s.Comment                           AS STL_Comment
FROM [Metrics].[MongoDBRightsizingRecommendations_STL] s
JOIN [Metrics].[MongoDBRightsizingRecommendations] n
    ON  n.ClusterKey = s.ClusterKey
    AND n.DayType    = s.DayType
    AND n.HourType   = s.HourType
    AND n.Month      = s.Month
WHERE s.Action != n.Action  ← only different ones
ORDER BY s.ClusterName, s.DayType
GO

-- Summary count
SELECT
    s.Action    AS STL_Action,
    n.Action    AS Normal_Action,
    COUNT(*)    AS ClusterCount
FROM [Metrics].[MongoDBRightsizingRecommendations_STL] s
JOIN [Metrics].[MongoDBRightsizingRecommendations] n
    ON  n.ClusterKey = s.ClusterKey
    AND n.DayType    = s.DayType
    AND n.HourType   = s.HourType
    AND n.Month      = s.Month
WHERE s.Action != n.Action
GROUP BY s.Action, n.Action
ORDER BY ClusterCount DESC
GO