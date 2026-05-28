SELECT
    p.ProcessType,
    COUNT(*) AS ProcessCount
FROM [MongoDB].[Process] p
JOIN [MongoDB].[Clusters] cl ON cl.ClustersKey = p.ClusterKey
WHERE cl.Name     = 'cdr-uat'
AND   p.IsDeleted = 0
GROUP BY p.ProcessType
ORDER BY p.ProcessType
GO