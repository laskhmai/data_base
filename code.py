SELECT
    ClustersKey,
    Name,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].regionName')   AS PrimaryRegion,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].providerName')  AS Provider,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].effectiveElectableSpecs.instanceSize') AS EffectiveSize,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].electableSpecs.instanceSize') AS ElectableSize,
    COUNT(*) OVER() AS RowCount
FROM [MongoDB].[Clusters]
WHERE Name = 'consumer-interops-uat'
GO

SELECT
    p.ProcessType,
    COUNT(*) AS ProcessCount
FROM [MongoDB].[Process] p
JOIN [MongoDB].[Clusters] cl
    ON cl.ClustersKey = p.ClusterKey
WHERE cl.Name     = 'consumer-interops-uat'
AND   p.IsDeleted = 0
GROUP BY p.ProcessType
ORDER BY ProcessCount DESC
GO

SELECT
    p.ProcessType,
    MAX(cn.Measurement) AS MaxConnections,
    AVG(cn.Measurement) AS AvgConnections,
    COUNT(DISTINCT p.ProcessId) AS ProcessCount
FROM [Metrics].[MongoDB_Connections_15M] cn
JOIN [MongoDB].[Process] p
    ON p.ProcessId = cn.[key]
JOIN [MongoDB].[Clusters] cl
    ON cl.ClustersKey = p.ClusterKey
WHERE cl.Name     = 'consumer-interops-uat'
AND   p.IsDeleted = 0
AND   cn.DateTime >= DATEADD(DAY, -7, GETDATE())
GROUP BY p.ProcessType
ORDER BY MaxConnections DESC
GO

SELECT
    ClusterName,
    InstanceSize,
    _date,
    _hour,
    ConnectionsMax,
    ConnUtilizationPct
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'consumer-interops-uat'
AND   ConnUtilizationPct > 100
ORDER BY ConnUtilizationPct DESC
GO