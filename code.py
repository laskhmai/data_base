-- Check raw MemAvailableMin values
SELECT TOP 5
    ClusterName,
    MIN(MemAvailableMin) AS MinAvailable,
    MAX(MemAvailableMin) AS MaxAvailable
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'coreapi-shared-prod'
GROUP BY ClusterName
GO