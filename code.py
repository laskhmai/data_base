-- Check if missing clusters exist in Process table
SELECT
    c.ClustersKey                       AS ClusterKey,
    c.Name                              AS ClusterName,
    c.StateName,
    COUNT(p.ProcessKey)                 AS ProcessCount,
    MIN(p.AuditUtc)                     AS FirstSeenInProcess,
    MAX(p.AuditUtc)                     AS LastSeenInProcess,
    SUM(CASE WHEN p.IsDeleted = 0
             THEN 1 ELSE 0 END)         AS ActiveProcesses,
    SUM(CASE WHEN p.IsDeleted = 1
             THEN 1 ELSE 0 END)         AS DeletedProcesses
FROM [MongoDB].[Clusters] c
LEFT JOIN [MongoDB].[Process] p
    ON p.ClusterKey = c.ClustersKey
WHERE c.StateName IN ('IDLE','UPDATING')
AND   c.Paused = 0
AND   c.ClustersKey NOT IN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    WHERE Month = '2026-05'
)
GROUP BY
    c.ClustersKey,
    c.Name,
    c.StateName
ORDER BY ProcessCount DESC, c.Name
GO

-- Summary
SELECT
    CASE
        WHEN COUNT(p.ProcessKey) = 0
        THEN 'No processes in Process table'
        WHEN SUM(CASE WHEN p.IsDeleted=0 THEN 1 ELSE 0 END) = 0
        THEN 'Only deleted processes'
        ELSE 'Has active processes'
    END                                 AS ProcessStatus,
    COUNT(DISTINCT c.ClustersKey)       AS ClusterCount
FROM [MongoDB].[Clusters] c
LEFT JOIN [MongoDB].[Process] p
    ON p.ClusterKey = c.ClustersKey
WHERE c.StateName IN ('IDLE','UPDATING')
AND   c.Paused = 0
AND   c.ClustersKey NOT IN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    WHERE Month = '2026-05'
)
GROUP BY
    c.ClustersKey,
    c.Name,
    c.StateName
ORDER BY ProcessStatus
GO