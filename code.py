-- Check most recent records inserted
SELECT TOP 20
    ProcessKey,
    Name,
    ProcessId,
    ProcessType,
    ClusterKey,
    ReplicaSetName,
    Version,
    ProcessUpdatedDate,
    IsDeleted
FROM [MongoDB].[Process]
ORDER BY ProcessUpdatedDate DESC


-- Count processes per org
SELECT
    o.Name          AS OrgName,
    COUNT(p.ProcessKey) AS ProcessCount
FROM [MongoDB].[Process] p
JOIN [MongoDB].[Organization] o ON o.OrgKey = p.OrgKey
WHERE p.IsDeleted = 0
GROUP BY o.Name
ORDER BY ProcessCount DESC


-- Check cloudea specifically
SELECT
    p.ProcessKey,
    p.Name,
    p.ProcessId,
    p.ProcessType,
    p.IsDeleted,
    p.ProcessUpdatedDate
FROM [MongoDB].[Process] p
JOIN [MongoDB].[Organization] o ON o.OrgKey = p.OrgKey
WHERE o.Name LIKE '%cloudea%'
AND p.IsDeleted = 0
ORDER BY p.ProcessUpdatedDate DESC


-- How many processes per cluster
SELECT
    c.Name          AS ClusterName,
    o.Name          AS OrgName,
    COUNT(p.ProcessKey) AS ProcessCount
FROM [MongoDB].[Process] p
JOIN [MongoDB].[Clusters] c      ON c.ClustersKey = p.ClusterKey
JOIN [MongoDB].[Organization] o  ON o.OrgKey = p.OrgKey
WHERE p.IsDeleted = 0
GROUP BY c.Name, o.Name
ORDER BY o.Name, c.Name



-- Check what was updated in last 24 hours
SELECT
    p.Name,
    p.ProcessType,
    o.Name          AS OrgName,
    p.ProcessUpdatedDate,
    p.IsDeleted
FROM [MongoDB].[Process] p
JOIN [MongoDB].[Organization] o ON o.OrgKey = p.OrgKey
WHERE p.ProcessUpdatedDate >= DATEADD(HOUR, -24, GETDATE())
ORDER BY p.ProcessUpdatedDate DESC