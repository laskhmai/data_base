-- Check duplicates in aggregated table
SELECT
    ClusterKey,
    _date,
    _hour,
    [type],
    businessHour,
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GROUP BY ClusterKey, _date, _hour, [type], businessHour
HAVING COUNT(*) > 1
ORDER BY RowCount DESC
GO

-- Summary
SELECT
    COUNT(*)                   AS TotalRows,
    COUNT(DISTINCT CONCAT(
        CAST(ClusterKey AS VARCHAR),
        CAST(_date AS VARCHAR),
        CAST(_hour AS VARCHAR),
        [type], businessHour
    ))                         AS UniqueRows,
    MAX(cnt)                   AS MaxDuplicatesPerRow
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
CROSS JOIN (
    SELECT MAX(RowCount) AS cnt FROM (
        SELECT COUNT(*) AS RowCount
        FROM [Metrics].[MongoDBRightsizingAggregated5Min]
        GROUP BY ClusterKey, _date, _hour, [type], businessHour
    ) x
) y
GO