-- See if SKU varies month to month
-- for autoscaling clusters
SELECT DISTINCT
    a.ClusterName,
    a.InstanceSize,
    FORMAT(a._date,'yyyy-MM') AS Month
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
JOIN [MongoDB].[Clusters] c
    ON c.ClustersKey = a.ClusterKey
WHERE JSON_VALUE(c.ReplicationSpecs,
    '$[0].regionConfigs[0].autoScaling.compute.enabled') = 'true'
AND FORMAT(a._date,'yyyy-MM') = '2026-06'
ORDER BY a.ClusterName, a.InstanceSize
GO