-- How many orgs fetched in yesterday's processor run
SELECT 
    o.Name                          AS OrgName,
    COUNT(DISTINCT p.ProjectKey)    AS Projects,
    COUNT(DISTINCT p.ClusterKey)    AS Clusters,
    COUNT(*)                        AS Processes,
    MAX(p.AuditUtc)                 AS LastUpdated
FROM [MongoDB].[Process] p
JOIN [MongoDB].[Organization] o ON o.OrgKey = p.OrgKey
WHERE CAST(p.AuditUtc AS DATE) = CAST(DATEADD(DAY,-1,GETDATE()) AS DATE)
AND p.IsDeleted = 0
GROUP BY o.Name
ORDER BY Processes DESC

-- Summary
SELECT 
    COUNT(DISTINCT o.OrgId)         AS TotalOrgs,
    COUNT(DISTINCT p.ProjectKey)    AS TotalProjects,
    COUNT(DISTINCT p.ClusterKey)    AS TotalClusters,
    COUNT(*)                        AS TotalProcesses
FROM [MongoDB].[Process] p
JOIN [MongoDB].[Organization] o ON o.OrgKey = p.OrgKey
WHERE CAST(p.AuditUtc AS DATE) = CAST(DATEADD(DAY,-1,GETDATE()) AS DATE)
AND p.IsDeleted = 0