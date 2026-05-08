-- How many clusters does cloudea org have?
SELECT
    c.ClustersKey,
    c.Name,
    c.StateName,
    p.Name AS ProjectName
FROM [MongoDB].[Clusters] c
JOIN [MongoDB].[Projects] p ON p.ProjectKey = c.ProjectKey
JOIN [MongoDB].[Organization] o ON o.OrgKey = p.OrgKey
WHERE o.Name LIKE '%cloudea%'
ORDER BY c.Name