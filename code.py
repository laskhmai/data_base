-- What does aggregated table show?
SELECT
    ClusterName,
    _date,
    _hour,
    [type],
    businessHour,
    ROUND(CpuAvg,    2) AS CpuAvg,
    ROUND(CpuMax,    2) AS CpuMax,
    ROUND(CpuAvgP95, 2) AS CpuAvgP95,
    ROUND(CpuMaxP95, 2) AS CpuMaxP95
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName LIKE '%cmsonc-eob-prod%'
AND   FORMAT(_date,'yyyy-MM') = '2026-05'
ORDER BY CpuMax DESC
GO