-- Check what data exists for this cluster
SELECT
    [type]        AS DayType,
    businessHour  AS HourType,
    COUNT(*)      AS RowCount,
    MIN(_date)    AS DataFrom,
    MAX(_date)    AS DataTo
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterKey = 19
GROUP BY [type], businessHour
ORDER BY [type], businessHour