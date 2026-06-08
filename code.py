-- Check if lqubase is in the cluster inventory
SELECT
    p.ClusterKey,
    cl.Name AS ClusterName,
    cl.State,
    cl.IsDeleted,
    JSON_VALUE(cl.ReplicationSpecs,
        '$[0].regionConfigs[0].effectiveElectableSpecs.instanceSize')
        AS InstanceSize
FROM [MongoDB].[Clusters] cl
JOIN [MongoDB].[Process] p
    ON p.ClusterKey = cl.ClustersKey
WHERE cl.Name LIKE '%lqubase%'
OR p.ClusterKey = 19