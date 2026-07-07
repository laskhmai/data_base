-- Check for duplicate rows in STL table
SELECT
    ClusterName,
    DayType,
    HourType,
    Month,
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingRecommendations_STL]
WHERE Month = '2026-06'
GROUP BY
    ClusterName, DayType, HourType, Month
HAVING COUNT(*) > 1
ORDER BY RowCount DESC
GO

-- How many duplicate slices?
SELECT
    SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS DuplicateSlices,
    SUM(CASE WHEN cnt = 1 THEN 1 ELSE 0 END) AS UniqueSlices
FROM (
    SELECT COUNT(*) AS cnt
    FROM [Metrics].[MongoDBRightsizingRecommendations_STL]
    WHERE Month = '2026-06'
    GROUP BY ClusterName, DayType, HourType, Month
) x
GO