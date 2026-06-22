-- CHECK 1: Table row counts
SELECT
    'Aggregated'      AS TableName,
    COUNT(*)          AS TotalRows,
    COUNT(DISTINCT ClusterKey) AS Clusters,
    MIN(_date)        AS DataFrom,
    MAX(_date)        AS DataTo
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
UNION ALL
SELECT
    'Recommendations',
    COUNT(*),
    COUNT(DISTINCT ClusterKey),
    NULL, NULL
FROM [Metrics].[MongoDBRightsizingRecommendations]
UNION ALL
SELECT
    'SimulatedMetrics',
    COUNT(*),
    COUNT(DISTINCT ClusterKey),
    MIN([Date]),
    MAX([Date])
FROM [Metrics].[MongoDBRightsizingSimulatedMetrics]
GO

-- CHECK 2: No duplicates in Recommendations
SELECT
    COUNT(*) AS TotalRows,
    COUNT(DISTINCT CONCAT(
        Month, CAST(ClusterKey AS VARCHAR),
        DayType, HourType
    ))        AS UniqueRows
FROM [Metrics].[MongoDBRightsizingRecommendations]
GO

-- CHECK 3: No duplicates in SimulatedMetrics
SELECT
    COUNT(*) AS TotalRows,
    COUNT(DISTINCT CONCAT(
        CAST(ClusterKey AS VARCHAR),
        CAST([Date] AS VARCHAR),
        CAST([Hour] AS VARCHAR),
        DayType, HourType, CurrentSku
    ))        AS UniqueRows
FROM [Metrics].[MongoDBRightsizingSimulatedMetrics]
GO

-- CHECK 4: Known cluster validation
SELECT
    ClusterName,
    DayType,
    HourType,
    CurrentSku,
    RecommendedSku,
    LowCpuSku,
    Action,
    ROUND(EstimatedMonthlySavings, 2) AS Savings,
    Comment
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE ClusterName IN (
    'cdr-uat',
    'cwih-cp-mgmt-prod',
    'consumer-interops-uat'
)
ORDER BY ClusterName, DayType, HourType
GO

-- CHECK 5: Cost summary
SELECT
    Month,
    Action,
    COUNT(DISTINCT ClusterKey)          AS Clusters,
    ROUND(SUM(Spend30days), 2)          AS CurrentSpend,
    ROUND(SUM(EstimatedMonthlySavings), 2) AS NetSavings
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE DayType  = 'Weekday'
AND   HourType = 'BusinessHours'
GROUP BY Month, Action
ORDER BY Action
GO

-- CHECK 6: Efficiency populated correctly
SELECT
    ClusterName,
    DayType,
    HourType,
    CASE WHEN CurrentEfficiency IS NOT NULL THEN 'Populated' ELSE 'NULL' END AS CurrentEff,
    CASE WHEN WithinEfficiency  IS NOT NULL THEN 'Populated' ELSE 'NULL' END AS WithinEff,
    CASE WHEN LowCpuEfficiency  IS NOT NULL THEN 'Populated' ELSE 'NULL' END AS LowCpuEff
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE ClusterName IN (
    'cdr-uat',
    'cwih-cp-mgmt-prod',
    'consumer-interops-uat'
)
ORDER BY ClusterName, DayType, HourType
GO

-- CHECK 7: Simulated metrics for known cluster
SELECT TOP 5
    ClusterName,
    [Date],
    [Hour],
    DayType,
    HourType,
    ROUND(CpuAvg, 2)         AS CpuAvg,
    ROUND(nCpuAvgWithin, 2)  AS ProjectedCpuWithin,
    ROUND(MemAvg, 2)         AS MemAvg,
    ROUND(nMemAvgWithin, 2)  AS ProjectedMemWithin
FROM [Metrics].[MongoDBRightsizingSimulatedMetrics]
WHERE ClusterName = 'cdr-uat'
ORDER BY [Date] DESC, [Hour]
GO