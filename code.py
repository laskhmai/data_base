-- How many shards does cdr-uat ACTUALLY have?
SELECT DISTINCT
    p.ClusterKey,
    cl.Name,
    p.ProcessId,
    p.ProcessType,
    p.ReplicaSetName
FROM [MongoDB].[Process] p
JOIN [MongoDB].[Clusters] cl ON cl.ClustersKey = p.ClusterKey
WHERE cl.Name     = 'cdr-uat'
AND   p.ProcessType = 'REPLICA_PRIMARY'
AND   p.IsDeleted   = 0
ORDER BY p.ReplicaSetName
GO