-- Check duplicates in STL recommendations table
SELECT
    ClusterName,
    DayType,
    HourType,
    Month,
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingRecommendations_STL]
WHERE Month = '2026-06'
GROUP BY
    ClusterName,
    DayType,
    HourType,
    Month
HAVING COUNT(*) > 1
ORDER BY RowCount DESC
GO

-- Summary
SELECT
    COUNT(*)    AS DuplicateSlices,
    SUM(RowCount - 1) AS ExtraRows
FROM (
    SELECT COUNT(*) AS RowCount
    FROM [Metrics].[MongoDBRightsizingRecommendations_STL]
    WHERE Month = '2026-06'
    GROUP BY ClusterName, DayType, HourType, Month
    HAVING COUNT(*) > 1
) x
GO