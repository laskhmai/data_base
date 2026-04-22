-- Check total process count
SELECT COUNT(*) AS TotalProcesses
FROM [MongoDB].[Process]

-- Check processes per org
SELECT 
    o.Name AS OrgName,
    COUNT(p.ProcessKey) AS ProcessCount
FROM [MongoDB].[Process] p
JOIN [MongoDB].[Organization] o 
    ON p.OrgKey = o.OrgKey
GROUP BY o.Name
ORDER BY ProcessCount DESC

-- Check latest processes inserted
SELECT TOP 20
    ProcessKey,
    Name,
    ProcessType,
    UserAlias,
    ProcessUpdatedDate
FROM [MongoDB].[Process]
ORDER BY ProcessKey DESC