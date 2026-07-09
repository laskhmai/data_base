SELECT
    r.ClusterName,
    r.CurrentSku,
    r.DayType,
    r.HourType,
    r.Action,
    ROUND(MAX(a.CpuMaxP95),  2)     AS CpuMaxP95,
    ROUND(MAX(a.CpuMaxP95)*2,2)     AS CpuMaxP95x2,
    ROUND(MAX(a.CpuMax),     2)     AS PeakCpuMax,
    ROUND(MAX(a.CpuMax)*2,   2)     AS PeakCpuMaxx2,
    CASE
        WHEN MAX(a.CpuMaxP95)*2 < 100
        AND  MAX(a.CpuMax)*2    > 100
        THEN 'Downsize but Peak unsafe ⚠️'
        WHEN MAX(a.CpuMaxP95)*2 < 100
        AND  MAX(a.CpuMax)*2    < 100
        THEN 'Downsize and Peak safe ✅'
        ELSE 'NoChange already'
    END                             AS PeakCheck
FROM [Metrics].[MongoDBRightsizingRecommendations_STL] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = r.ClusterKey
    AND a.[type]       = r.DayType
    AND a.businessHour = r.HourType
    AND FORMAT(a._date,'yyyy-MM') = r.Month
WHERE r.Month  = '2026-06'
AND   r.Action = 'Downsize'
GROUP BY
    r.ClusterName, r.CurrentSku,
    r.DayType, r.HourType, r.Action
ORDER BY PeakCheck DESC
GO