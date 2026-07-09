SELECT
    r.ClusterName,
    r.CurrentSku,
    r.DayType,
    r.HourType,
    ROUND(MAX(a.CpuMaxP95),  2)     AS CpuMaxP95,
    ROUND(MAX(a.CpuMax),     2)     AS PeakCpuMax,
    ROUND(MAX(a.CpuMax)*2,   2)     AS PeakCpuMaxx2,
    -- How many hours hit > 50%?
    SUM(CASE WHEN a.CpuMax > 50
             THEN 1 ELSE 0 END)     AS HoursAbove50,
    COUNT(*)                        AS TotalHours,
    -- What % of hours had high CPU?
    ROUND(100.0 *
        SUM(CASE WHEN a.CpuMax > 50
                 THEN 1 ELSE 0 END)
        / COUNT(*), 2)              AS PctHoursAbove50
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
    r.DayType, r.HourType
HAVING
    MAX(a.CpuMaxP95) * 2 < 100
AND MAX(a.CpuMax)   * 2 > 100
ORDER BY PctHoursAbove50 DESC
GO