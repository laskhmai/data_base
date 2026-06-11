-- Check for duplicates in SimulatedMetrics
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

-- Summary counts
SELECT
    COUNT(*)                    AS TotalRows,
    COUNT(DISTINCT CONCAT(
        CAST(ClusterKey AS VARCHAR),
        CAST([Date] AS VARCHAR),
        CAST([Hour] AS VARCHAR),
        DayType, HourType, CurrentSku
    ))                          AS UniqueRows,
    COUNT(*) - COUNT(DISTINCT CONCAT(
        CAST(ClusterKey AS VARCHAR),
        CAST([Date] AS VARCHAR),
        CAST([Hour] AS VARCHAR),
        DayType, HourType, CurrentSku
    ))                          AS DuplicateRows,
    MAX(cnt)                    AS MaxDuplicatesPerRow
FROM [Metrics].[MongoDBRightsizingSimulatedMetrics]
CROSS JOIN (
    SELECT MAX(RowCount) AS cnt
    FROM (
        SELECT COUNT(*) AS RowCount
        FROM [Metrics].[MongoDBRightsizingSimulatedMetrics]
        GROUP BY ClusterKey, [Date], [Hour],
                 DayType, HourType, CurrentSku
    ) x
) y
GO