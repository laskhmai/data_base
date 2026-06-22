-- Check when these clusters last reported data
SELECT
    ClusterName,
    InstanceSize,
    ProviderName,
    MIN(_date)          AS FirstSeen,
    MAX(_date)          AS LastSeen,
    COUNT(*)            AS TotalRows,
    DATEDIFF(DAY,
        MAX(_date),
        CAST(GETDATE() AS DATE))  AS DaysSinceLastData
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName IN (
    'supp-ecosystem-prod',
    'nlp-milan-prod',
    'digital-enrollment-platform-dev-dev',
    'digital-enrollment-platform-qa',
    'digital-enrollment-platform-prod',
    'digital-enrollment-platform-stage-staging'
)
GROUP BY ClusterName, InstanceSize, ProviderName
ORDER BY LastSeen DESC
GO

-- Check why Python skipped these 2 clusters
-- They ARE in aggregated (M50 and M40)
-- but NOT in recommendations

SELECT
    a.ClusterName,
    a.InstanceSize,
    a.ProviderName,
    a.RegionName,
    COUNT(*)            AS TotalRows,
    MAX(a._date)        AS LastDate
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
WHERE a.ClusterName IN (
    'supp-ecosystem-prod',
    'nlp-milan-prod'
)
GROUP BY
    a.ClusterName,
    a.InstanceSize,
    a.ProviderName,
    a.RegionName
GO

-- Also check if their SKU exists in MetaConfig
SELECT
    mc.Instance,
    mc.Provider,
    mc.Region,
    mc.Tier,
    mc.vCores,
    mc.MemorySizeGB
FROM [Analytics].[MongoDBMetaConfig] mc
WHERE mc.Instance IN ('M50', 'M40')
AND   mc.Provider = 'AZURE'
AND   mc.Region   = 'US_EAST_2'
AND   mc.Tier NOT IN ('Free','Flex')
GO