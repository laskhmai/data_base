-- Check auto-scaling clusters side by side
-- Configured SKU vs Actual SKU used
SELECT
    c.Name                                  AS ClusterName,
    c.ClustersKey,
    JSON_VALUE(c.ReplicationSpecs,
        '$[0].regionConfigs[0].electableSpecs.instanceSize')
                                            AS ConfiguredSku,
    JSON_VALUE(c.ReplicationSpecs,
        '$[0].regionConfigs[0].autoScaling.compute.enabled')
                                            AS AutoScaleEnabled,
    JSON_VALUE(c.ReplicationSpecs,
        '$[0].regionConfigs[0].autoScaling.compute.minInstanceSize')
                                            AS AutoScaleMin,
    JSON_VALUE(c.ReplicationSpecs,
        '$[0].regionConfigs[0].autoScaling.compute.maxInstanceSize')
                                            AS AutoScaleMax,
    a.ActualSku,
    a.MinSkuSeen,
    a.MaxSkuSeen,
    CASE
        WHEN JSON_VALUE(c.ReplicationSpecs,
            '$[0].regionConfigs[0].electableSpecs.instanceSize')
             != a.ActualSku
        THEN 'DIFFERENT ⚠️'
        ELSE 'Same ✅'
    END                                     AS SkuMatch
FROM [MongoDB].[Clusters] c
JOIN (
    SELECT
        ClusterKey,
        MAX(InstanceSize)                   AS ActualSku,
        MIN(InstanceSize)                   AS MinSkuSeen,
        MAX(InstanceSize)                   AS MaxSkuSeen
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
    GROUP BY ClusterKey
) a ON a.ClusterKey = c.ClustersKey
WHERE c.StateName IN ('IDLE','UPDATING')
AND   c.Paused     = 0
AND   JSON_VALUE(c.ReplicationSpecs,
        '$[0].regionConfigs[0].autoScaling.compute.enabled') = 'true'
ORDER BY SkuMatch DESC, c.Name
GO

-- Summary count
SELECT
    CASE
        WHEN JSON_VALUE(c.ReplicationSpecs,
            '$[0].regionConfigs[0].electableSpecs.instanceSize')
             != a.ActualSku
        THEN 'DIFFERENT ⚠️'
        ELSE 'Same ✅'
    END                                     AS SkuMatch,
    COUNT(*)                                AS ClusterCount
FROM [MongoDB].[Clusters] c
JOIN (
    SELECT
        ClusterKey,
        MAX(InstanceSize)                   AS ActualSku
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
    GROUP BY ClusterKey
) a ON a.ClusterKey = c.ClustersKey
WHERE c.StateName IN ('IDLE','UPDATING')
AND   c.Paused     = 0
AND   JSON_VALUE(c.ReplicationSpecs,
        '$[0].regionConfigs[0].autoScaling.compute.enabled') = 'true'
GROUP BY
    CASE
        WHEN JSON_VALUE(c.ReplicationSpecs,
            '$[0].regionConfigs[0].electableSpecs.instanceSize')
             != a.ActualSku
        THEN 'DIFFERENT ⚠️'
        ELSE 'Same ✅'
    END
ORDER BY ClusterCount DESC
GO