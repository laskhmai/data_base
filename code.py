SELECT
    r.ClusterName,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action,
    r.MaxCpuProcessId,          ← she will check this
    r.MaxMemProcessId,          ← she will check this
    ROUND(r.AvgCpuMax,   2)    AS AvgCpuMax,
    ROUND(r.PeakCpuMax,  2)    AS PeakCpuMax,
    ROUND(MAX(a.CpuMaxP95),  2) AS CpuMaxP95,
    ROUND(MAX(a.CpuMaxP95)*2,2) AS CpuMaxP95x2,
    ROUND(r.EstimatedMonthlySavings,2) AS Savings,
    r.Comment
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = r.ClusterKey
    AND a.[type]       = r.DayType
    AND a.businessHour = r.HourType
WHERE r.Month    = '2026-06'
AND   r.DayType  = 'Weekday'
AND   r.HourType = 'BusinessHours'
AND   r.ClusterName IN (
    'cwih-cp-mgmt-prod',
    'cmsonc-eob-prod-cluster',
    '[your Downsize cluster]'   ← replace this
)
GROUP BY
    r.ClusterName, r.CurrentSku,
    r.RecommendedSku, r.Action,
    r.MaxCpuProcessId, r.MaxMemProcessId,
    r.AvgCpuMax, r.PeakCpuMax,
    r.EstimatedMonthlySavings, r.Comment
ORDER BY r.ClusterName
GO