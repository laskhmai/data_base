-- See exactly how many times each
-- cluster appears in the 304 rows
SELECT
    ClusterName,
    DayType,
    HourType,
    COUNT(*) AS AppearCount
FROM [Metrics].[MongoDBRightsizingRecommendations_STL] s
JOIN (
    SELECT
        ClusterKey,
        [type]                          AS DayType,
        businessHour                    AS HourType,
        FORMAT(_date,'yyyy-MM')         AS Month,
        ROUND(MAX(CpuMaxP95)*2, 2)     AS CpuMaxP95x2,
        ROUND(MAX(CpuAvgP95)*2, 2)     AS CpuAvgP95x2
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
    GROUP BY ClusterKey, [type], businessHour,
             FORMAT(_date,'yyyy-MM')
) r
    ON  r.ClusterKey = s.ClusterKey
    AND r.DayType    = s.DayType
    AND r.HourType   = s.HourType
    AND r.Month      = s.Month
WHERE s.Month = '2026-06'
AND   s.Action != CASE
    WHEN r.CpuMaxP95x2 < 100
    AND  r.CpuAvgP95x2 < 100
    THEN 'Downsize' ELSE 'NoChange' END
GROUP BY ClusterName, DayType, HourType
HAVING COUNT(*) > 1
ORDER BY AppearCount DESC
GO