-- Check what we got
SELECT
    FORMAT(_date,'yyyy-MM')         AS Month,
    COUNT(*)                        AS TotalRows,
    COUNT(DISTINCT ClusterKey)      AS Clusters,
    MIN(_date)                      AS DataFrom,
    MAX(_date)                      AS DataTo
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GROUP BY FORMAT(_date,'yyyy-MM')
ORDER BY Month
GO

-- Check duplicates in aggregated table
SELECT
    ClusterKey,
    _date,
    _hour,
    [type],
    businessHour,
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GROUP BY
    ClusterKey,
    _date,
    _hour,
    [type],
    businessHour
HAVING COUNT(*) > 1
ORDER BY RowCount DESC
GO

-- Summary
SELECT
    COUNT(*)                    AS TotalRows,
    COUNT(DISTINCT CONCAT(
        CAST(ClusterKey AS VARCHAR),
        CAST(_date      AS VARCHAR),
        CAST(_hour      AS VARCHAR),
        [type],
        businessHour
    ))                          AS UniqueRows,
    COUNT(*) - COUNT(DISTINCT CONCAT(
        CAST(ClusterKey AS VARCHAR),
        CAST(_date      AS VARCHAR),
        CAST(_hour      AS VARCHAR),
        [type],
        businessHour
    ))                          AS DuplicateRows
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GO