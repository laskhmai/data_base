-- Show which slice is missing per cluster
SELECT
    ClusterKey,
    ClusterName,
    MAX(CASE WHEN DayType='Weekday'
             AND HourType='BusinessHours'
             THEN 1 ELSE 0 END)    AS HasBusinessHours,
    MAX(CASE WHEN DayType='Weekday'
             AND HourType='NonBusinessHours'
             THEN 1 ELSE 0 END)    AS HasNonBusinessHours,
    MAX(CASE WHEN DayType='Weekend'
             AND HourType='Weekend'
             THEN 1 ELSE 0 END)    AS HasWeekend
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month = '2026-05'
GROUP BY ClusterKey, ClusterName
HAVING COUNT(*) < 3
ORDER BY ClusterName
GO