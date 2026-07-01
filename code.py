-- Find cleanest Downsize cluster
-- Low CPU, clearly safe
SELECT TOP 1
    r.ClusterName,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action,
    ROUND(r.AvgCpuMax,   2)         AS AvgCpuMax,
    ROUND(r.PeakCpuMax,  2)         AS PeakCpuMax,
    ROUND(MAX(a.CpuMaxP95),  2)     AS CpuMaxP95,
    ROUND(MAX(a.CpuMaxP95)*2,2)     AS CpuMaxP95x2,
    ROUND(r.EstimatedMonthlySavings,2) AS Savings
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = r.ClusterKey
    AND a.[type]       = r.DayType
    AND a.businessHour = r.HourType
WHERE r.Month      = '2026-06'
AND   r.DayType    = 'Weekday'
AND   r.HourType   = 'BusinessHours'
AND   r.Action     = 'Downsize'
GROUP BY
    r.ClusterName, r.CurrentSku,
    r.RecommendedSku, r.Action,
    r.AvgCpuMax, r.PeakCpuMax,
    r.EstimatedMonthlySavings
ORDER BY MAX(a.CpuMaxP95) ASC  -- lowest P95 = clearest case
GO