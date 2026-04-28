SELECT clusterid, ReplicationSpecs
FROM [MongoDB].[Clusters]
WHERE ReplicationSpecs LIKE '%"diskSizeGB": 0%'
   OR ReplicationSpecs LIKE '%"diskSizeGB": 1%'
   OR ReplicationSpecs LIKE '%shared%';