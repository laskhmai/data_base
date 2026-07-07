-- Check duplicates in STL table only
-- No joins needed — simple GROUP BY
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

-- How many duplicate slices total
SELECT
    COUNT(*)              AS DuplicateSlices,
    SUM(RowCount - 1)     AS ExtraRows
FROM (
    SELECT COUNT(*) AS RowCount
    FROM [Metrics].[MongoDBRightsizingRecommendations_STL]
    WHERE Month = '2026-06'
    GROUP BY
        ClusterName,
        DayType,
        HourType,
        Month
    HAVING COUNT(*) > 1
) x
GO