-- Summary of recommendations by Action
SELECT
    Action,
    COUNT(DISTINCT ClusterKey)          AS UniqueClusters,
    COUNT(*)                            AS TotalSlices,
    ROUND(SUM(EstimatedMonthlySavings), 2) AS TotalEstimatedSavings,
    ROUND(AVG(EstimatedMonthlySavings), 2) AS AvgSavingsPerSlice,
    ROUND(SUM(Spend30days), 2)          AS TotalActualSpend
FROM [Metrics].[MongoDBRightsizingRecommendations_STL]
WHERE Month = '2026-06'
GROUP BY Action
ORDER BY Action
GO

-- Detailed Downsize savings by cluster
SELECT
    ClusterName,
    CurrentSku,
    RecommendedSku,
    DayType,
    HourType,
    ROUND(EstimatedMonthlySavings, 2)   AS EstimatedMonthlySavings,
    ROUND(Spend30days, 2)               AS ActualSpend30days,
    Comment
FROM [Metrics].[MongoDBRightsizingRecommendations_STL]
WHERE Month  = '2026-06'
AND   Action = 'Downsize'
ORDER BY EstimatedMonthlySavings DESC
GO

-- Detailed Upsize cost impact by cluster
SELECT
    ClusterName,
    CurrentSku,
    RecommendedSku,
    DayType,
    HourType,
    ROUND(EstimatedMonthlySavings, 2)   AS EstimatedMonthlyCost,
    ROUND(Spend30days, 2)               AS ActualSpend30days,
    Comment
FROM [Metrics].[MongoDBRightsizingRecommendations_STL]
WHERE Month  = '2026-06'
AND   Action = 'Upsize'
ORDER BY EstimatedMonthlySavings ASC
GO