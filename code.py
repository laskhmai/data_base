-- =============================================
-- CHECK 1: Recommendations table duplicates
-- =============================================
SELECT
    Month,
    ClusterKey,
    ClusterName,
    DayType,
    HourType,
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingRecommendations]
GROUP BY Month, ClusterKey, ClusterName, DayType, HourType
HAVING COUNT(*) > 1
ORDER BY RowCount DESC
GO

-- Summary
SELECT
    COUNT(*)                                        AS TotalRows,
    COUNT(DISTINCT CONCAT(
        Month, CAST(ClusterKey AS VARCHAR),
        DayType, HourType
    ))                                              AS UniqueRows,
    COUNT(*) - COUNT(DISTINCT CONCAT(
        Month, CAST(ClusterKey AS VARCHAR),
        DayType, HourType
    ))                                              AS DuplicateRows
FROM [Metrics].[MongoDBRightsizingRecommendations]
GO

-- =============================================
-- CHECK 2: SimulatedMetrics table duplicates
-- =============================================
SELECT
    ClusterKey,
    ClusterName,
    [Date],
    [Hour],
    DayType,
    HourType,
    CurrentSku,
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingSimulatedMetrics]
GROUP BY
    ClusterKey, ClusterName,
    [Date], [Hour],
    DayType, HourType, CurrentSku
HAVING COUNT(*) > 1
ORDER BY RowCount DESC
GO

-- Summary
SELECT
    COUNT(*)                   AS TotalRows,
    COUNT(DISTINCT CONCAT(
        CAST(ClusterKey AS VARCHAR),
        CAST([Date] AS VARCHAR),
        CAST([Hour] AS VARCHAR),
        DayType, HourType, CurrentSku
    ))                         AS UniqueRows,
    COUNT(*) - COUNT(DISTINCT CONCAT(
        CAST(ClusterKey AS VARCHAR),
        CAST([Date] AS VARCHAR),
        CAST([Hour] AS VARCHAR),
        DayType, HourType, CurrentSku
    ))                         AS DuplicateRows
FROM [Metrics].[MongoDBRightsizingSimulatedMetrics]
GO