-- Check if sharedrefapp cluster exists at all
SELECT 
    ClustersKey, Name, ClusterId, StateName
FROM [MongoDB].[Clusters]
WHERE Name LIKE '%sharedrefapp%'

-- Also check what clusters we DO have for cloudea org
SELECT 
    c.ClustersKey, c.Name, c.StateName,
    p.Name AS ProjectName,
    o.Name AS OrgName
FROM [MongoDB].[Clusters] c
JOIN [MongoDB].[Projects] p ON p.ProjectKey = c.ProjectKey
JOIN [MongoDB].[Organization] o ON o.OrgKey = p.OrgKey
WHERE o.Name LIKE '%cloudea%'
ORDER BY c.Name

-- Check if sharedrefapp processes exist in our Process table
SELECT
    ProcessKey,
    Name,
    ProcessId,
    ProcessType,
    ClusterKey,
    ReplicaSetName,
    IsDeleted
FROM [MongoDB].[Process]
WHERE Name LIKE '%sharedrefapp%'
   OR ProcessId LIKE '%sharedrefapp%'
   OR ReplicaSetName LIKE '%sharedrefapp%'