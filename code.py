-- Find 3 clusters in Aggregated but NOT in Recommendations
SELECT DISTINCT
    a.ClusterKey,
    a.ClusterName,
    a.InstanceSize,
    a.ProviderName,
    a.RegionName
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
WHERE a.ClusterKey NOT IN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
)
GO

-- Find 4 clusters in Recommendations but NOT in SimulatedMetrics
SELECT DISTINCT
    r.ClusterKey,
    r.ClusterName,
    r.CurrentSku,
    r.ProviderName,
    r.RegionName
FROM [Metrics].[MongoDBRightsizingRecommendations] r
WHERE r.ClusterKey NOT IN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingSimulatedMetrics]
)
GO

-- Check if those 4 clusters have June data in aggregated
SELECT
    a.ClusterKey,
    a.ClusterName,
    COUNT(*) AS JuneRows,
    MIN(a._date) AS DataFrom,
    MAX(a._date) AS DataTo
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
WHERE a.ClusterKey IN (
    SELECT DISTINCT r.ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations] r
    WHERE r.ClusterKey NOT IN (
        SELECT DISTINCT ClusterKey
        FROM [Metrics].[MongoDBRightsizingSimulatedMetrics]
    )
)
GROUP BY a.ClusterKey, a.ClusterName
ORDER BY JuneRows
GO