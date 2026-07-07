-- Compare the two active Cluster0 records
SELECT
    ClustersKey,
    Name,
    StateName,
    Paused,
    CreateDate,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].electableSpecs.instanceSize')
                                    AS InstanceSize,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].providerName')
                                    AS Provider,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].regionName')
                                    AS Region,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].autoScaling.compute.enabled')
                                    AS AutoScale,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].autoScaling.compute.minInstanceSize')
                                    AS MinSku,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].autoScaling.compute.maxInstanceSize')
                                    AS MaxSku
FROM [MongoDB].[Clusters]
WHERE Name IN ('Cluster0', 'HCaaS-Dev', 'HCaaS-dev',
               'epms-ckd3-dev', 'ecr-ckd3-dev')
AND   Paused = 0
ORDER BY Name, CreateDate
GO