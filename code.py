-- Check all downsize clusters in May
-- where CpuMax is dangerously high
SELECT
    r.ClusterName,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action,
    ROUND(MAX(a.CpuMax), 2)     AS MaxCpuSpike,
    ROUND(AVG(a.CpuAvg), 2)    AS AvgCpu
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey = r.ClusterKey
    AND FORMAT(a._date,'yyyy-MM') = r.Month
WHERE r.Month      = '2026-05'
AND   r.Action     = 'Downsize'
AND   r.DayType    = 'Weekday'
AND   r.HourType   = 'BusinessHours'
GROUP BY
    r.ClusterName,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action
HAVING MAX(a.CpuMax) > 60   -- clusters with dangerous spikes
ORDER BY MaxCpuSpike DESC
GO