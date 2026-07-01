-- Action summary
SELECT
    Action,
    COUNT(DISTINCT ClusterKey) AS Clusters,
    ROUND(SUM(EstimatedMonthlySavings),2) AS NetSavings
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE DayType  = 'Weekday'
AND   HourType = 'BusinessHours'
GROUP BY Action
ORDER BY Action
GO

-- Two key clusters
SELECT
    ClusterName,
    DayType,
    HourType,
    Action,
    RecommendedSku,
    ROUND(PeakCpuMax,2) AS PeakCpuMax,
    ROUND(EstimatedMonthlySavings,2) AS Savings
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE ClusterName IN (
    'cdr-uat',
    'cmsonc-eob-prod-cluster'
)
ORDER BY ClusterName, DayType, HourType
GO