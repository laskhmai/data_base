-- Simpler: aggregated table + recommendations
SELECT
    a.ClusterName,
    a.MaxCpuProcessId               AS ProcessId,
    ROUND(a.CpuAvg,    2)          AS CpuAvg,
    ROUND(a.CpuAvgP95, 2)          AS CpuAvgP95,
    ROUND(a.CpuMax,    2)          AS CpuMax,
    ROUND(a.CpuMaxP95, 2)          AS CpuMaxP95,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
JOIN [Metrics].[MongoDBRightsizingRecommendations] r
    ON  r.ClusterKey = a.ClusterKey
    AND r.Month      = FORMAT(a._date, 'yyyy-MM')
    AND r.DayType    = a.[type]
    AND r.HourType   = a.businessHour
WHERE a.ClusterName = 'cmsonc-eob-prod-cluster'
AND   FORMAT(a._date,'yyyy-MM') = '2026-05'
ORDER BY a.CpuMax DESC
GO