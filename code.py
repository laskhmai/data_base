-- SUMMARY FIXED
SELECT
    Verdict,
    COUNT(*)                            AS ClusterSlices,
    COUNT(DISTINCT ClusterName)         AS UniqueClusters
FROM (
    SELECT
        n.ClusterName                   AS ClusterName,
        CASE
            WHEN n.Action = CASE
                WHEN r.CpuMaxP95x2 < 100
                AND  r.CpuAvgP95x2 < 100
                THEN 'Downsize' ELSE 'NoChange' END
            AND  s.Action != CASE
                WHEN r.CpuMaxP95x2 < 100
                AND  r.CpuAvgP95x2 < 100
                THEN 'Downsize' ELSE 'NoChange' END
            THEN 'Normal Correct'

            WHEN s.Action = CASE
                WHEN r.CpuMaxP95x2 < 100
                AND  r.CpuAvgP95x2 < 100
                THEN 'Downsize' ELSE 'NoChange' END
            AND  n.Action != CASE
                WHEN r.CpuMaxP95x2 < 100
                AND  r.CpuAvgP95x2 < 100
                THEN 'Downsize' ELSE 'NoChange' END
            THEN 'STL Correct'

            ELSE 'Edge Case'
        END                             AS Verdict
    FROM [Metrics].[MongoDBRightsizingRecommendations] n
    JOIN [Metrics].[MongoDBRightsizingRecommendations_STL] s
        ON  s.ClusterKey = n.ClusterKey
        AND s.DayType    = n.DayType
        AND s.HourType   = n.HourType
        AND s.Month      = n.Month
    JOIN (
        SELECT
            ClusterKey,
            [type]                      AS DayType,
            businessHour                AS HourType,
            FORMAT(_date,'yyyy-MM')     AS Month,
            ROUND(MAX(CpuMaxP95)*2, 2)  AS CpuMaxP95x2,
            ROUND(MAX(CpuAvgP95)*2, 2)  AS CpuAvgP95x2
        FROM [Metrics].[MongoDBRightsizingAggregated5Min]
        WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
        GROUP BY
            ClusterKey,
            [type],
            businessHour,
            FORMAT(_date,'yyyy-MM')
    ) r
        ON  r.ClusterKey = n.ClusterKey
        AND r.DayType    = n.DayType
        AND r.HourType   = n.HourType
        AND r.Month      = n.Month
    WHERE n.Action != s.Action
) x
GROUP BY Verdict
ORDER BY ClusterSlices DESC
GO