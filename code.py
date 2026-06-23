-- Clusters with May data but NO recommendations
SELECT DISTINCT
    a.ClusterKey,
    a.ClusterName,
    a.InstanceSize,
    a.ProviderName,
    a.RegionName
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
WHERE FORMAT(a._date,'yyyy-MM') = '2026-05'
AND   a.ClusterKey NOT IN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    WHERE Month = '2026-05'
)
ORDER BY a.InstanceSize
GO