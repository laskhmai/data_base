SELECT
    r.ClusterName,
    r.DayType,
    r.HourType,
    r.Action,
    r.CpuRec,
    r.ConnRec,
    r.Comment,
    ROUND(r.AvgCpuMax,          2) AS AvgCpuMax,
    ROUND(r.PeakCpuMax,         2) AS PeakCpuMax,
    ROUND(r.ConnUtilizationPct, 2) AS ConnUtil,
    ROUND(r.MemUtilizationPct,  2) AS MemUtil,
    ROUND(MAX(a.CpuMaxP95),     2) AS CpuMaxP95,
    ROUND(MAX(a.CpuMaxP95)*2,   2) AS CpuMaxP95x2
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = r.ClusterKey
    AND a.[type]       = r.DayType
    AND a.businessHour = r.HourType
WHERE r.ClusterName = 'consumer-interops-prod'
AND   r.Month       = '2026-07'
GROUP BY
    r.ClusterName, r.DayType, r.HourType,
    r.Action, r.CpuRec, r.ConnRec, r.Comment,
    r.AvgCpuMax, r.PeakCpuMax,
    r.ConnUtilizationPct, r.MemUtilizationPct
ORDER BY r.DayType, r.HourType
GO