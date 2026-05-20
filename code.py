-- Check what tiers exist currently
SELECT Tier, COUNT(*) AS RowCount
FROM [Analytics].[MongoDBMetaConfig]
GROUP BY Tier
ORDER BY Tier

-- Check if Burstable update was done
SELECT SkuName, Tier
FROM [Analytics].[MongoDBMetaConfig]
WHERE SkuName IN ('M10','M20','M30')
ORDER BY SkuName

-- Check if M0/M2/M5 exist in real clusters
SELECT DISTINCT InstanceSize, COUNT(*) AS Count
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE InstanceSize IN ('M0','M2','M5')
GROUP BY InstanceSize

-- What regions are in our aggregation table?
SELECT DISTINCT 
    ProviderName,
    RegionName,
    COUNT(DISTINCT ClusterKey) AS Clusters
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE InstanceSize IS NOT NULL
GROUP BY ProviderName, RegionName
ORDER BY ProviderName, RegionName