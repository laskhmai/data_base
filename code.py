-- Check if duplicates exist in 5Min table
SELECT
    ClusterKey,
    ClusterName,
    _date,
    _hour,
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterKey = 330
GROUP BY
    ClusterKey,
    ClusterName,
    _date,
    _hour
HAVING COUNT(*) > 1
ORDER BY _date, _hour
GO