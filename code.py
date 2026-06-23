-- How many clusters have May data
SELECT
    COUNT(DISTINCT ClusterKey) AS ClustersWithMayData,
    MIN(_date)                  AS From,
    MAX(_date)                  AS To
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE FORMAT(_date,'yyyy-MM') = '2026-05'
GO

-- Check recommendations count
SELECT
    Month,
    Action,
    COUNT(DISTINCT ClusterKey) AS Clusters,
    COUNT(*)                   AS TotalRows
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month = '2026-05'
GROUP BY Month, Action
ORDER BY Action
GO