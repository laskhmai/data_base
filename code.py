-- Exact slices per cluster
SELECT
    SliceCount,
    COUNT(*)  AS ClusterCount
FROM (
    SELECT
        ClusterKey,
        ClusterName,
        COUNT(*) AS SliceCount
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    GROUP BY ClusterKey, ClusterName
) x
GROUP BY SliceCount
ORDER BY SliceCount
GO

SELECT
    ClusterKey,
    ClusterName,
    COUNT(*) AS SliceCount,
    STRING_AGG(DayType + '-' + HourType, ' | ')
              AS SlicesTheyHave
FROM [Metrics].[MongoDBRightsizingRecommendations]
GROUP BY ClusterKey, ClusterName
HAVING COUNT(*) < 3
ORDER BY ClusterName
GO