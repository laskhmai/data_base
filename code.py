-- Full list of M0/free tier clusters
SELECT 
    DISTINCT 
    ClusterName,
    InstanceSize,
    COUNT(*) OVER (PARTITION BY ClusterName) AS RowCount
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE InstanceSize IN ('M0', 'M2', 'M5')
ORDER BY InstanceSize, ClusterName

-- Summary count
SELECT 
    InstanceSize,
    COUNT(DISTINCT ClusterName) AS ClusterCount,
    COUNT(DISTINCT ClusterKey)  AS UniqueClusterKeys,
    COUNT(*)                    AS TotalRows
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE InstanceSize IN ('M0', 'M2', 'M5')
GROUP BY InstanceSize