-- Step 1: Get its ClusterKey
SELECT DISTINCT ClusterKey, ClusterName
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ClusterName = 'liquibase-mongodb-dev1'

-- Step 2: Test what InstanceSize it got
SELECT DISTINCT ClusterName, InstanceSize
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ClusterName = 'liquibase-mongodb-dev1'

-- Step 3: Test COALESCE directly on its Clusters row
SELECT 
    ClustersKey,
    Name,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].effectiveElectableSpecs.instanceSize') AS Path1,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].electableSpecs.instanceSize')          AS Path2,
    LEFT(ReplicationSpecs, 300) AS JsonPreview
FROM [MongoDB].[Clusters]
WHERE Name = 'liquibase-mongodb-dev1'