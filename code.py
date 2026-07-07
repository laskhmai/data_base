SELECT
    CASE
        WHEN s.Action = CASE
            WHEN r.CpuMaxP95x2 < 100
            AND  r.CpuAvgP95x2 < 100
            THEN 'Downsize' ELSE 'NoChange' END
        THEN 'Matches Expected'
        ELSE 'Differs from Expected'
    END                                 AS Status,
    COUNT(DISTINCT s.ClusterKey)        AS UniqueClusters,
    COUNT(*)                            AS TotalSlices
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
    GROUP BY ClusterKey,[type],
             businessHour,
             FORMAT(_date,'yyyy-MM')
) r
    ON  r.ClusterKey = s.ClusterKey
    AND r.DayType    = s.DayType
    AND r.HourType   = s.HourType
    AND r.Month      = s.Month
WHERE s.Month = '2026-06'
GROUP BY
    CASE
        WHEN s.Action = CASE
            WHEN r.CpuMaxP95x2 < 100
            AND  r.CpuAvgP95x2 < 100
            THEN 'Downsize' ELSE 'NoChange' END
        THEN 'Matches Expected'
        ELSE 'Differs from Expected'
    END
ORDER BY Status
GO