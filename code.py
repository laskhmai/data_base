-- STEP 1: Which 50 clusters are missing?
SELECT
    c.ClustersKey                   AS ClusterKey,
    c.Name                          AS ClusterName,
    c.StateName,
    c.Paused,
    -- Check if they have any data in aggregated table
    CASE WHEN a.ClusterKey IS NOT NULL
         THEN 'Has Metrics'
         ELSE 'No Metrics'
    END                             AS MetricsStatus,
    -- Check if they have May data specifically
    CASE WHEN a2.ClusterKey IS NOT NULL
         THEN 'Has May Data'
         ELSE 'No May Data'
    END                             AS MayDataStatus
FROM [MongoDB].[Clusters] c
LEFT JOIN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
) a ON a.ClusterKey = c.ClustersKey
LEFT JOIN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE FORMAT(_date,'yyyy-MM') = '2026-05'
) a2 ON a2.ClusterKey = c.ClustersKey
WHERE c.StateName IN ('IDLE','UPDATING')
AND   c.Paused = 0
AND   c.ClustersKey NOT IN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    WHERE Month = '2026-05'
)
ORDER BY MetricsStatus, MayDataStatus, c.Name
GO

-- STEP 2: Summary count
SELECT
    CASE WHEN a.ClusterKey IS NOT NULL
         THEN 'Has Metrics'
         ELSE 'No Metrics'
    END                             AS MetricsStatus,
    CASE WHEN a2.ClusterKey IS NOT NULL
         THEN 'Has May Data'
         ELSE 'No May Data'
    END                             AS MayDataStatus,
    COUNT(*)                        AS ClusterCount
FROM [MongoDB].[Clusters] c
LEFT JOIN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
) a ON a.ClusterKey = c.ClustersKey
LEFT JOIN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE FORMAT(_date,'yyyy-MM') = '2026-05'
) a2 ON a2.ClusterKey = c.ClustersKey
WHERE c.StateName IN ('IDLE','UPDATING')
AND   c.Paused = 0
AND   c.ClustersKey NOT IN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    WHERE Month = '2026-05'
)
GROUP BY
    CASE WHEN a.ClusterKey IS NOT NULL
         THEN 'Has Metrics'
         ELSE 'No Metrics' END,
    CASE WHEN a2.ClusterKey IS NOT NULL
         THEN 'Has May Data'
         ELSE 'No May Data' END
ORDER BY MetricsStatus, MayDataStatus
GO