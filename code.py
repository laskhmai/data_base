-- Compare AVG vs SUM for cdr-uat
-- to understand the impact

SELECT
    ClusterName,
    _date,
    _hour,
    ROUND(AVG(CpuAvg), 2)   AS CurrentAvgMethod,
    ROUND(SUM(CpuAvg), 2)   AS SumMethod,
    COUNT(*)                 AS ProcessCount
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'cdr-uat'
AND   _date       = '2026-06-16'
AND   _hour       = 0
GROUP BY ClusterName, _date, _hour
GO