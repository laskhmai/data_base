-- What SKU/Provider/Region is cdr-uat actually using?
SELECT
    cl.Name         AS ClusterName,
    JSON_VALUE(cl.ReplicationSpecs,
        '$[0].regionConfigs[0].effectiveElectableSpecs.instanceSize')
                    AS InstanceSize,
    JSON_VALUE(cl.ReplicationSpecs,
        '$[0].regionConfigs[0].providerName')
                    AS Provider,
    JSON_VALUE(cl.ReplicationSpecs,
        '$[0].regionConfigs[0].regionName')
                    AS Region
FROM [MongoDB].[Clusters] cl
WHERE cl.Name = 'cdr-uat'
GO