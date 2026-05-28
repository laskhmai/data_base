SELECT SkuName, Provider, Region, ConnectionLimit
FROM [Analytics].[MongoDBMetaConfig]
WHERE SkuName LIKE '%M30%'
OR    SkuName LIKE '%M40%'
ORDER BY SkuName
GO