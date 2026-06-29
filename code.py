-- Check missing clusters against RAW table
SELECT
    c.ClustersKey                       AS ClusterKey,
    c.Name                              AS ClusterName,
    c.StateName,
    -- Check aggregated table
    CASE WHEN a.ClusterKey IS NOT NULL
         THEN 'Has Aggregated'
         ELSE 'No Aggregated'
    END                                 AS AggregatedStatus,
    -- Check raw CPU metric table
    CASE WHEN r.ClusterKey IS NOT NULL
         THEN 'Has Raw Metrics'
         ELSE 'No Raw Metrics'
    END                                 AS RawMetricStatus,
    -- Min/Max dates in raw table
    r.RawFrom,
    r.RawTo
FROM [MongoDB].[Clusters] c
LEFT JOIN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE FORMAT(_date,'yyyy-MM') = '2026-05'
) a ON a.ClusterKey = c.ClustersKey
LEFT JOIN (
    SELECT
        p.ClusterKey,
        MIN(CAST(m.DateTime AS DATE))   AS RawFrom,
        MAX(CAST(m.DateTime AS DATE))   AS RawTo
    FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M] m
    JOIN [MongoDB].[Process] p
        ON  p.ProcessId  = m.[Key]
        AND p.IsDeleted  = 0
    WHERE FORMAT(m.DateTime,'yyyy-MM') = '2026-05'
    GROUP BY p.ClusterKey
) r ON r.ClusterKey = c.ClustersKey
WHERE c.StateName IN ('IDLE','UPDATING')
AND   c.Paused     = 0
AND   c.ClustersKey NOT IN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    WHERE Month = '2026-05'
)
ORDER BY RawMetricStatus DESC, AggregatedStatus, c.Name
GO

-- Summary
SELECT
    CASE WHEN a.ClusterKey IS NOT NULL
         THEN 'Has Aggregated'
         ELSE 'No Aggregated' END       AS AggregatedStatus,
    CASE WHEN r.ClusterKey IS NOT NULL
         THEN 'Has Raw Metrics'
         ELSE 'No Raw Metrics' END      AS RawMetricStatus,
    COUNT(*)                            AS ClusterCount
FROM [MongoDB].[Clusters] c
LEFT JOIN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE FORMAT(_date,'yyyy-MM') = '2026-05'
) a ON a.ClusterKey = c.ClustersKey
LEFT JOIN (
    SELECT DISTINCT p.ClusterKey
    FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M] m
    JOIN [MongoDB].[Process] p
        ON  p.ProcessId  = m.[Key]
        AND p.IsDeleted  = 0
    WHERE FORMAT(m.DateTime,'yyyy-MM') = '2026-05'
) r ON r.ClusterKey = c.ClustersKey
WHERE c.StateName IN ('IDLE','UPDATING')
AND   c.Paused     = 0
AND   c.ClustersKey NOT IN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    WHERE Month = '2026-05'
)
GROUP BY
    CASE WHEN a.ClusterKey IS NOT NULL
         THEN 'Has Aggregated'
         ELSE 'No Aggregated' END,
    CASE WHEN r.ClusterKey IS NOT NULL
         THEN 'Has Raw Metrics'
         ELSE 'No Raw Metrics' END
ORDER BY RawMetricStatus DESC, AggregatedStatus
GO