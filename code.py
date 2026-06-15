Expected 284 clusters
Got 280 clusters → 4 clusters missing

Possible reason:
4 clusters may not have Weekday BusinessHours data
in aggregated table for June 2026

Check:
SELECT ClusterName FROM aggregated
WHERE ClusterKey NOT IN (
    SELECT ClusterKey FROM recommendations
    WHERE DayType='Weekday'
    AND HourType='BusinessHours'
)