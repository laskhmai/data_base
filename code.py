-- Check every hour of data for cdr-dev in June
SELECT
    _date,
    _hour,
    [type]                          AS DayType,
    businessHour                    AS HourType,
    ROUND(CpuAvg,    2)            AS CpuAvg,
    ROUND(CpuMax,    2)            AS CpuMax,
    ROUND(CpuMaxP95, 2)            AS CpuMaxP95,
    ROUND(CpuAvgP95, 2)            AS CpuAvgP95,
    MaxCpuProcessId
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'cdr-dev'
AND   FORMAT(_date,'yyyy-MM') = '2026-06'
ORDER BY _date, _hour
GO