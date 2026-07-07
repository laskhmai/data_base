-- Count clusters where STL_Action differs from Expected_Action
SELECT
    CASE
        WHEN s.Action = CASE
            WHEN MAX(a.CpuMaxP95)*2 < 100
            AND  MAX(a.CpuAvgP95)*2 < 100
            THEN 'Downsize' ELSE 'NoChange' END
        THEN 'STL Matches Expected ✅'
        ELSE 'STL Differs from Expected ❌'
    END                                     AS Status,
    s.Action                                AS STL_Action,
    CASE
        WHEN MAX(a.CpuMaxP95)*2 < 100
        AND  MAX(a.CpuAvgP95)*2 < 100
        THEN 'Downsize' ELSE 'NoChange'
    END                                     AS Expected_Action,
    COUNT(DISTINCT s.ClusterKey)            AS UniqueClusters,
    COUNT(*)                                AS TotalSlices
FROM [Metrics].[MongoDBRightsizingRecommendations_STL] s
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = s.ClusterKey
    AND a.[type]       = s.DayType
    AND a.businessHour = s.HourType
    AND FORMAT(a._date,'yyyy-MM') = s.Month
WHERE s.Month = '2026-06'
GROUP BY
    s.ClusterKey,
    s.Action,
    CASE
        WHEN MAX(a.CpuMaxP95)*2 < 100
        AND  MAX(a.CpuAvgP95)*2 < 100
        THEN 'Downsize' ELSE 'NoChange'
    END
ORDER BY Status DESC
GO

-- Summary only
SELECT
    CASE
        WHEN s.Action = CASE
            WHEN r.CpuMaxP95x2 < 100
            AND  r.CpuAvgP95x2 < 100
            THEN 'Downsize' ELSE 'NoChange' END
        THEN 'Matches Expected ✅'
        ELSE 'Differs from Expected ❌'
    END                                     AS Status,
    COUNT(DISTINCT s.ClusterKey)            AS UniqueClusters,
    COUNT(*)                                AS TotalSlices
FROM [Metrics].[MongoDBRightsizingRecommendations_STL] s
JOIN (
    SELECT
        ClusterKey,
        [type]                              AS DayType,
        businessHour                        AS HourType,
        FORMAT(_date,'yyyy-MM')             AS Month,
        ROUND(MAX(CpuMaxP95)*2, 2)         AS CpuMaxP95x2,
        ROUND(MAX(CpuAvgP95)*2, 2)         AS CpuAvgP95x2
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
    GROUP BY
        ClusterKey, [type], businessHour,
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
        THEN 'Matches Expected ✅'
        ELSE 'Differs from Expected ❌'
    END
ORDER BY Status
GO