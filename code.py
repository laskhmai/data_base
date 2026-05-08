SELECT
    c.ClustersKey,
    c.Name        AS ClusterName,
    p.Name        AS ProjectName,
    o.Name        AS OrgName,
    o.OrgId
FROM [MongoDB].[Clusters] c
JOIN [MongoDB].[Projects] p      ON p.ProjectKey = c.ProjectKey
JOIN [MongoDB].[Organization] o  ON o.OrgKey     = p.OrgKey
WHERE c.ClustersKey = 8