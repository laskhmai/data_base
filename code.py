-- Check 1 month of metric data for cdr-dev (key 326)
-- See what data we have collected
SELECT
    FORMAT(_date,'yyyy-MM-dd')      AS Date,
    COUNT(*)                        AS HourlyRows,
    ROUND(AVG(CpuAvg),    2)       AS AvgCpuAvg,
    ROUND(AVG(CpuMax),    2)       AS AvgCpuMax,
    ROUND(MAX(CpuMax),    2)       AS PeakCpuMax,
    ROUND(AVG(CpuMaxP95), 2)       AS CpuMaxP95,
    MIN(_hour)                      AS FirstHour,
    MAX(_hour)                      AS LastHour
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'cdr-dev'
AND   FORMAT(_date,'yyyy-MM') = '2026-06'
ORDER BY _date
GO

-- Summary for cdr-dev June
SELECT
    ClusterName,
    COUNT(DISTINCT _date)           AS DaysWithData,
    COUNT(*)                        AS TotalHourlyRows,
    MIN(_date)                      AS DataFrom,
    MAX(_date)                      AS DataTo,
    ROUND(AVG(CpuMax),    2)       AS AvgCpuMax,
    ROUND(MAX(CpuMax),    2)       AS PeakCpuMax,
    ROUND(AVG(CpuMaxP95), 2)       AS AvgCpuMaxP95
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'cdr-dev'
AND   FORMAT(_date,'yyyy-MM') = '2026-06'
GROUP BY ClusterName
GO