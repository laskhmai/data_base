-- Check which clusters have less than 3 slices
SELECT
    ClusterKey,
    ClusterName,
    COUNT(*) AS SliceCount
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month = '2026-05'
GROUP BY ClusterKey, ClusterName
HAVING COUNT(*) < 3
ORDER BY SliceCount
GO