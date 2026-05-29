-- See which SKUs have impossible values
SELECT DISTINCT
    s.InstanceSize,
    s.ProviderName,
    s.RegionName,
    MAX(s.MemResidentMaxPct)    AS MaxMemPct,
    MAX(s.ConnUtilizationPct)   AS MaxConnPct,
    MAX(s.MemResidentMax)       AS MaxMemMB,
    m.MemorySizeGB,
    m.ConnectionLimit
FROM [Metrics].[MongoDBRightsizingAggregated5Min] s
LEFT JOIN [Analytics].[MongoDBMetaConfig] m
    ON  m.Instance = s.InstanceSize
    AND m.Provider = s.ProviderName
    AND m.Tier NOT IN ('Free','Flex')
WHERE s.MemResidentMaxPct > 100
OR    s.ConnUtilizationPct > 100
GROUP BY
    s.InstanceSize, s.ProviderName, s.RegionName,
    m.MemorySizeGB, m.ConnectionLimit
ORDER BY s.InstanceSize
GO