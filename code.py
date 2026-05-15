SELECT 
    ClustersKey,
    Name,
    StateName,
    JSON_VALUE(ReplicationSpecs, 
        '$[0].regionConfigs[0].electableSpecs.instanceSize') AS ElectablePath,
    JSON_VALUE(ReplicationSpecs, 
        '$[0].regionConfigs[1].electableSpecs.instanceSize') AS ElectablePath_Region1
FROM [MongoDB].[Clusters]
WHERE Name LIKE 'cwih-ptscheduling%'
ORDER BY Name, ClustersKey

SELECT DISTINCT ClusterKey, ClusterName
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ClusterName LIKE 'cwih-ptscheduling%'