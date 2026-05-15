SELECT 
    Name,
    JSON_VALUE(ReplicationSpecs, 
        '$[0].regionConfigs[0].effectiveElectableSpecs.instanceSize') AS Path1,
    JSON_VALUE(ReplicationSpecs, 
        '$[0].regionConfigs[0].electableSpecs.instanceSize')          AS Path2,
    JSON_VALUE(ReplicationSpecs, 
        '$[0].regionConfigs[0].analyticsSpecs.instanceSize')          AS AnalyticsPath,
    LEFT(ReplicationSpecs, 200) AS JsonPreview
FROM [MongoDB].[Clusters]
WHERE Name LIKE 'cwih-ptscheduling%'