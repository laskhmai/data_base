-- How many orgs, projects, clusters, processes
SELECT 
    COUNT(DISTINCT o.OrgId)    AS TotalOrgs,
    COUNT(DISTINCT p.ProjectKey) AS TotalProjects,
    COUNT(DISTINCT p.ClusterKey) AS TotalClusters,
    COUNT(DISTINCT p.ProcessKey) AS TotalProcesses
FROM [MongoDB].[Process] p
JOIN [MongoDB].[Organization] o ON o.OrgKey = p.OrgKey
WHERE p.IsDeleted = 0

-- Breakdown by Org
SELECT 
    o.Name                          AS OrgName,
    o.OrgId,
    COUNT(DISTINCT p.ProjectKey)    AS Projects,
    COUNT(DISTINCT p.ClusterKey)    AS Clusters,
    COUNT(DISTINCT p.ProcessKey)    AS Processes
FROM [MongoDB].[Process] p
JOIN [MongoDB].[Organization] o ON o.OrgKey = p.OrgKey
WHERE p.IsDeleted = 0
GROUP BY o.Name, o.OrgId
ORDER BY Processes DESC