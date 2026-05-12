SELECT 
    p.ProjectId,
    p.Name AS ProjectName,
    o.Name AS OrgName
FROM [MongoDB].[Projects] p
JOIN [MongoDB].[Organization] o ON o.OrgKey = p.OrgKey
WHERE p.ProjectId IN (
    '6142eb53fe93fc1d517441f2',
    '67be301fda5ce57afb557b4f',
    '67dc59d8670f0d4c507cad39',
    '67d894b744cf8d1ac2db73a8'
)