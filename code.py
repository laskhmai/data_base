-- Find which day has CpuMaxP95 = 82.17%
SELECT
    _date,
    _hour,
    ROUND(CpuAvg,    2)    AS CpuAvg,
    ROUND(CpuMax,    2)    AS CpuMax,
    ROUND(CpuAvgP95, 2)    AS CpuAvgP95,
    ROUND(CpuMaxP95, 2)    AS CpuMaxP95,
    DATEPART(WEEK, _date)  AS WeekNumber
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName  = 'cdr-uat'
AND   FORMAT(_date,'yyyy-MM') = '2026-05'
AND   CpuMaxP95    > 50
ORDER BY CpuMaxP95 DESC
GO