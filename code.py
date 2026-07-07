-- Side by side: configured min SKU vs actual SKU used
SELECT
    c.Name                                  AS ClusterName,
    -- Configured minimum SKU (what we currently use)
    JSON_VALUE(c.ReplicationSpecs,
        '$[0].regionConfigs[0].electableSpecs.instanceSize')
                                            AS ConfiguredMinSku,
    -- Auto scale max SKU
    JSON_VALUE(c.ReplicationSpecs,
        '$[0].regionConfigs[0].autoScaling.compute.maxInstanceSize')
                                            AS AutoScaleMaxSku,
    -- Actual SKU from aggregated table (what cluster really ran on)
    a.ActualSku,
    a.MinSkuSeen,
    a.MaxSkuSeen,
    -- Are they different?
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
        -- Most common SKU used in month
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

-- Summary
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
GO