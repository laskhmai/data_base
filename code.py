-- Check for duplicates
SELECT
    Month,
    ClusterKey,
    DayType,
    HourType,
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingRecommendations]
GROUP BY Month, ClusterKey, DayType, HourType
HAVING COUNT(*) > 1
ORDER BY RowCount DESC

-- Summary
SELECT
    COUNT(*)                                    AS TotalRows,
    COUNT(DISTINCT CONCAT(ClusterKey, DayType, HourType)) AS UniqueSlices,
    MAX(cnt) AS MaxDuplicates
FROM (
    SELECT ClusterKey, DayType, HourType, COUNT(*) AS cnt
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    GROUP BY ClusterKey, DayType, HourType
) x
GO