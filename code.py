-- Quick join test before building notebook
SELECT 
    h.ClusterName,
    h.InstanceSize,
    h.ProviderName,
    h.RegionName,
    m.Tier,
    m.vCores,
    m.MemorySizeGB,
    m.CostPrHour
FROM [Metrics].[MongoDBRightsizingAggregatedHourly] h
JOIN [Analytics].[MongoDBMetaConfig] m
    ON  m.SkuName  = h.InstanceSize
    AND m.Provider = h.ProviderName
    AND m.Region   = h.RegionName
WHERE h.InstanceSize IS NOT NULL
GROUP BY 
    h.ClusterName, h.InstanceSize,
    h.ProviderName, h.RegionName,
    m.Tier, m.vCores, 
    m.MemorySizeGB, m.CostPrHour
ORDER BY h.ClusterName