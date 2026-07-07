-- Check Clusters table for autoscale config
-- ReplicationSpecs JSON has autoscaling info
SELECT
    Name                                AS ClusterName,
    StateName,
    -- Check if autoscaling is in JSON
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].electableSpecs.instanceSize')
                                        AS MinInstanceSize,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].autoScaling.compute.minInstanceSize')
                                        AS AutoScaleMin,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].autoScaling.compute.maxInstanceSize')
                                        AS AutoScaleMax,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].autoScaling.compute.enabled')
                                        AS AutoScaleEnabled
FROM [MongoDB].[Clusters]
WHERE StateName IN ('IDLE','UPDATING')
AND   Paused = 0
ORDER BY Name
GO

-- Summary: how many have autoscaling enabled
SELECT
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].autoScaling.compute.enabled')
                                        AS AutoScaleEnabled,
    COUNT(*)                            AS ClusterCount
FROM [MongoDB].[Clusters]
WHERE StateName IN ('IDLE','UPDATING')
AND   Paused = 0
GROUP BY
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].autoScaling.compute.enabled')
GO