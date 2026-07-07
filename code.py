SELECT
    Name,
    ClustersKey,
    StateName,
    Paused,
    CreateDate
FROM [MongoDB].[Clusters]
WHERE Name IN (
    'HCaaS-dev',
    'epms-ckd3-dev',
    'Cluster0',
    'ecr-ckd3-dev'
)
ORDER BY Name, CreateDate
GO