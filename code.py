SELECT
    Month,
    Action,
    COUNT(DISTINCT ClusterKey)          AS ClusterCount,
    ROUND(SUM(Spend30days), 2)          AS TotalCurrentSpend,
    ROUND(SUM(
        CASE WHEN Action = 'Downsize'
             THEN EstimatedMonthlySavings
             ELSE 0 END), 2)            AS TotalSavings,
    ROUND(SUM(
        CASE WHEN Action = 'Upsize'
             THEN ABS(EstimatedMonthlySavings)
             ELSE 0 END), 2)            AS TotalAdditionalCost,
    ROUND(SUM(EstimatedMonthlySavings), 2) AS NetSavings
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE DayType    = 'Weekday'
AND   HourType   = 'BusinessHours'
GROUP BY Month, Action
ORDER BY Month, Action
GO

-- Overall summary
SELECT
    Month,
    COUNT(DISTINCT ClusterKey)             AS TotalClusters,
    ROUND(SUM(Spend30days), 2)             AS TotalCurrentSpend,
    ROUND(SUM(
        CASE WHEN Action = 'Downsize'
             THEN EstimatedMonthlySavings
             ELSE 0 END), 2)              AS PotentialSavings,
    ROUND(SUM(
        CASE WHEN Action = 'Upsize'
             THEN ABS(EstimatedMonthlySavings)
             ELSE 0 END), 2)              AS AdditionalCost,
    ROUND(SUM(
        CASE WHEN Action = 'Downsize'
             THEN EstimatedMonthlySavings
             WHEN Action = 'Upsize'
             THEN EstimatedMonthlySavings
             ELSE 0 END), 2)              AS NetMonthlySavings
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE DayType  = 'Weekday'
AND   HourType = 'BusinessHours'
GROUP BY Month
GO