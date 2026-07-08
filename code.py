-- From ALL 327 clusters:
-- How many have autoscaling
-- How many have different configured vs actual SKU

SELECT
    COUNT(*)                                AS TotalClusters,
    -- Autoscaling stats
    SUM(CASE
        WHEN JSON_VALUE(c.ReplicationSpecs,
            '$[0].regionConfigs[0].autoScaling.compute.enabled') = 'true'
        THEN 1 ELSE 0 END)                  AS AutoScaleEnabled,
    SUM(CASE
        WHEN JSON_VALUE(c.ReplicationSpecs,
            '$[0].regionConfigs[0].autoScaling.compute.enabled') = 'false'
        THEN 1 ELSE 0 END)                  AS AutoScaleDisabled,
    SUM(CASE
        WHEN JSON_VALUE(c.ReplicationSpecs,
            '$[0].regionConfigs[0].autoScaling.compute.enabled') IS NULL
        THEN 1 ELSE 0 END)                  AS AutoScaleNull,
    -- SKU match stats (autoscale clusters only)
    SUM(CASE
        WHEN JSON_VALUE(c.ReplicationSpecs,
            '$[0].regionConfigs[0].autoScaling.compute.enabled') = 'true'
        AND  JSON_VALUE(c.ReplicationSpecs,
            '$[0].regionConfigs[0].electableSpecs.instanceSize')
             != a.ActualSku
        THEN 1 ELSE 0 END)                  AS AutoScale_DifferentSku,
    SUM(CASE
        WHEN JSON_VALUE(c.ReplicationSpecs,
            '$[0].regionConfigs[0].autoScaling.compute.enabled') = 'true'
        AND  JSON_VALUE(c.ReplicationSpecs,
            '$[0].regionConfigs[0].electableSpecs.instanceSize')
             = a.ActualSku
        THEN 1 ELSE 0 END)                  AS AutoScale_SameSku
FROM [MongoDB].[Clusters] c
JOIN (
    SELECT ClusterKey, MAX(InstanceSize) AS ActualSku
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
    GROUP BY ClusterKey
) a ON a.ClusterKey = c.ClustersKey
WHERE c.StateName IN ('IDLE','UPDATING')
AND   c.Paused = 0
GO