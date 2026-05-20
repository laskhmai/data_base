-- Total clusters in Clusters table
SELECT COUNT(DISTINCT ClustersKey) AS TotalClusters
FROM [MongoDB].[Clusters]

-- How many have data in aggregation table
SELECT COUNT(DISTINCT ClusterKey) AS InAggregation
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]

-- How many match MetaConfig
SELECT COUNT(DISTINCT h.ClusterKey) AS MatchMetaConfig
FROM [Metrics].[MongoDBRightsizingAggregatedHourly] h
JOIN [Analytics].[MongoDBMetaConfig] m
    ON  m.SkuName   = h.InstanceSize
    AND m.Provider  = h.ProviderName
    AND m.Region    = h.RegionName

-- Find missing clusters — why they don't match
SELECT DISTINCT
    h.ClusterKey,
    h.ClusterName,
    h.InstanceSize,
    h.ProviderName,
    h.RegionName,
    h.ProcessType,
    CASE
        WHEN h.InstanceSize IS NULL 
            THEN 'NULL InstanceSize'
        WHEN h.InstanceSize IN ('M0','M2','M5') 
            THEN 'Free/Flex tier'
        WHEN h.ProcessType != 'REPLICA_PRIMARY' 
            THEN 'Not Primary — ' + h.ProcessType
        WHEN m.SkuName IS NULL 
            THEN 'Not in MetaConfig'
        ELSE 'Unknown'
    END AS Reason
FROM [Metrics].[MongoDBRightsizingAggregatedHourly] h
LEFT JOIN [Analytics].[MongoDBMetaConfig] m
    ON  m.SkuName   = h.InstanceSize
    AND m.Provider  = h.ProviderName
    AND m.Region    = h.RegionName
WHERE h.ClusterKey NOT IN (
    SELECT DISTINCT h2.ClusterKey
    FROM [Metrics].[MongoDBRightsizingAggregatedHourly] h2
    JOIN [MongoDB].[Process] p ON p.ProcessId = h2.ProcessId
    JOIN [Analytics].[MongoDBMetaConfig] m2
        ON  m2.SkuName  = h2.InstanceSize
        AND m2.Provider = h2.ProviderName
        AND m2.Region   = h2.RegionName
    WHERE h2.ProcessType = 'REPLICA_PRIMARY'
    AND   h2.InstanceSize NOT IN ('M0','M2','M5')
)
GROUP BY
    h.ClusterKey, h.ClusterName,
    h.InstanceSize, h.ProviderName,
    h.RegionName, h.ProcessType,
    m.SkuName
ORDER BY Reason, h.ClusterName