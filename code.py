SELECT
    ClusterName, InstanceSize,
    MAX(MemResidentMaxPct)  AS MaxPct,
    MAX(MemResidentP95Pct)  AS P95Pct,
    AVG(MemResidentAvgPct)  AS AvgPct
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterKey = 80
GROUP BY ClusterName, InstanceSize
GO