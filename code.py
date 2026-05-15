SELECT 
    h.ClusterName,
    h.ClusterKey,
    c.ReplicationSpecs,
    COUNT(*) AS NullRows
FROM [Metrics].[MongoDBRightsizingAggregatedHourly] h
JOIN [MongoDB].[Clusters] c 
    ON h.ClusterKey = c.ClustersKey
WHERE h.InstanceSize IS NULL
GROUP BY h.ClusterName, h.ClusterKey, c.ReplicationSpecs
ORDER BY NullRows DESC