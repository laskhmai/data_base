-- Duplicates by ClusterKey + DayType + HourType
SELECT
    ClusterKey,
    ClusterName,
    DayType,
    HourType,
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingRecommendations]
GROUP BY ClusterKey, ClusterName, DayType, HourType
HAVING COUNT(*) > 1
ORDER BY RowCount DESC

-- Show the actual duplicate rows
SELECT *
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE ClusterKey IN (
    SELECT ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    GROUP BY ClusterKey, DayType, HourType
    HAVING COUNT(*) > 1
)
ORDER BY ClusterKey, DayType, HourType

-- Should be exactly 855 = 285 clusters × 3 slices
SELECT
    COUNT(*)                    AS TotalRows,
    COUNT(DISTINCT ClusterKey)  AS UniqueClusters,
    COUNT(*) / COUNT(DISTINCT ClusterKey) AS SlicesPerCluster
FROM [Metrics].[MongoDBRightsizingRecommendations]