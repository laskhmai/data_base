-- Does cwh-cp-mgmt-prod have 2 rows in Clusters table?
SELECT
    ClustersKey,
    Name,
    COUNT(*) AS RowCount
FROM [MongoDB].[Clusters]
WHERE Name = 'cwh-cp-mgmt-prod'
GROUP BY ClustersKey, Name
GO

-- What region does cwh-cp-mgmt-prod actually use?
SELECT
    cl.Name,
    JSON_VALUE(cl.ReplicationSpecs,
        '$[0].regionConfigs[0].regionName')  AS PrimaryRegion,
    JSON_VALUE(cl.ReplicationSpecs,
        '$[0].regionConfigs[0].providerName') AS Provider,
    JSON_VALUE(cl.ReplicationSpecs,
        '$[0].regionConfigs[0].effectiveElectableSpecs.instanceSize') AS EffectiveSize,
    JSON_VALUE(cl.ReplicationSpecs,
        '$[0].regionConfigs[0].electableSpecs.instanceSize') AS ElectableSize
FROM [MongoDB].[Clusters] cl
WHERE cl.Name = 'cwh-cp-mgmt-prod'
GO