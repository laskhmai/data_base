SELECT
    COUNT(DISTINCT ClusterKey)      AS ClustersAbove100Pct,
    COUNT(DISTINCT ClusterKey) * 100.0 /
    (SELECT COUNT(DISTINCT ClusterKey)
     FROM [Metrics].[MongoDBRightsizingAggregated5Min]) AS PctOfTotal
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE MemResidentMaxPct > 100
GO

-- Detail per cluster
SELECT
    ClusterName,
    InstanceSize,
    ROUND(MAX(MemResidentMaxPct),     2) AS MaxMemPct,
    ROUND(AVG(MemResidentAvgPct),     2) AS AvgMemPct,
    ROUND(MIN(MemAvailableMin)/1024,  2) AS MinFreeMemMB
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE MemResidentMaxPct > 100
GROUP BY ClusterName, InstanceSize
ORDER BY MaxMemPct DESC
GO