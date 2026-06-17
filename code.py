-- Check duplicates in aggregated table
SELECT
    ClusterKey,
    ClusterName,
    _date,
    _hour,
    [type],
    businessHour,
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GROUP BY
    ClusterKey,
    ClusterName,
    _date,
    _hour,
    [type],
    businessHour
HAVING COUNT(*) > 1
ORDER BY RowCount DESC
GO

-- Summary
SELECT
    COUNT(*)         AS TotalRows,
    COUNT(DISTINCT CONCAT(
        CAST(ClusterKey  AS VARCHAR),
        CAST(_date       AS VARCHAR),
        CAST(_hour       AS VARCHAR),
        [type],
        businessHour
    ))               AS UniqueRows,
    COUNT(*) - COUNT(DISTINCT CONCAT(
        CAST(ClusterKey  AS VARCHAR),
        CAST(_date       AS VARCHAR),
        CAST(_hour       AS VARCHAR),
        [type],
        businessHour
    ))               AS DuplicateRows
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GO