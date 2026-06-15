-- Step 1: How many UNIQUE clusters are missing?
SELECT
    COUNT(DISTINCT ClusterKey) AS MissingClusters,
    COUNT(DISTINCT ClusterName) AS UniqueName
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterKey NOT IN (
    SELECT ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    WHERE DayType  = 'Weekday'
    AND   HourType = 'BusinessHours'
)
GO

-- Step 2: What SKUs are these missing clusters on?
SELECT DISTINCT
    InstanceSize,
    COUNT(DISTINCT ClusterKey) AS ClusterCount
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterKey NOT IN (
    SELECT ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    WHERE DayType  = 'Weekday'
    AND   HourType = 'BusinessHours'
)
GROUP BY InstanceSize
ORDER BY ClusterCount DESC
GO