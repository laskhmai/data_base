SELECT
    s.ClusterName,
    s.DayType,
    s.HourType,
    s.CurrentSku
FROM [Metrics].[MongoDBRightsizingRecommendations_STL] s
WHERE s.Month = '2026-06'
AND NOT EXISTS (
    SELECT 1
    FROM [Metrics].[MongoDBRightsizingRecommendations] n
    WHERE n.ClusterKey = s.ClusterKey
    AND   n.DayType    = s.DayType
    AND   n.HourType   = s.HourType
    AND   n.Month      = s.Month
)
ORDER BY s.ClusterName
GO

-- Count of missing
SELECT COUNT(*) AS MissingRows
FROM [Metrics].[MongoDBRightsizingRecommendations_STL] s
WHERE s.Month = '2026-06'
AND NOT EXISTS (
    SELECT 1
    FROM [Metrics].[MongoDBRightsizingRecommendations] n
    WHERE n.ClusterKey = s.ClusterKey
    AND   n.DayType    = s.DayType
    AND   n.HourType   = s.HourType
    AND   n.Month      = s.Month
)
GO