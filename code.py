-- Show all 137 clusters where STL differs from Expected
SELECT
    s.ClusterName,
    s.CurrentSku,
    s.DayType,
    s.HourType,
    s.Action                            AS STL_Action,
    CASE
        WHEN r.CpuMaxP95x2 < 100
        AND  r.CpuAvgP95x2 < 100
        THEN 'Downsize'
        ELSE 'NoChange'
    END                                 AS Expected_Action,
    ROUND(r.CpuMaxP95,  2)             AS CpuMaxP95,
    ROUND(r.CpuMaxP95x2,2)             AS CpuMaxP95x2,
    ROUND(r.CpuAvgP95,  2)             AS CpuAvgP95,
    ROUND(r.PeakCpuMax, 2)             AS PeakCpuMax,
    -- Why different?
    CASE
        WHEN s.Action = 'Downsize'
        AND  r.CpuMaxP95x2 >= 100
        THEN 'STL Downsizing when P95×2 > 100% ❌'
        WHEN s.Action = 'NoChange'
        AND  r.CpuMaxP95x2 < 100
        AND  r.CpuAvgP95x2 < 100
        THEN 'STL blocking safe Downsize ⚠️'
        WHEN s.Action = 'Upsize'
        THEN 'STL Upsizing — check if correct'
        ELSE 'Other difference'
    END                                 AS Reason
FROM [Metrics].[MongoDBRightsizingRecommendations_STL] s
JOIN (
    SELECT
        ClusterKey,
        [type]                          AS DayType,
        businessHour                    AS HourType,
        FORMAT(_date,'yyyy-MM')         AS Month,
        ROUND(MAX(CpuMaxP95),  2)      AS CpuMaxP95,
        ROUND(MAX(CpuMaxP95)*2,2)      AS CpuMaxP95x2,
        ROUND(MAX(CpuAvgP95),  2)      AS CpuAvgP95,
        ROUND(MAX(CpuAvgP95)*2,2)      AS CpuAvgP95x2,
        ROUND(MAX(CpuMax),     2)      AS PeakCpuMax
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
AND   s.Action != CASE
    WHEN r.CpuMaxP95x2 < 100
    AND  r.CpuAvgP95x2 < 100
    THEN 'Downsize' ELSE 'NoChange' END

-- Summary of reasons
ORDER BY Reason, s.ClusterName
GO

-- Summary count by reason
SELECT
    CASE
        WHEN s.Action = 'Downsize'
        AND  r.CpuMaxP95x2 >= 100
        THEN 'STL Downsizing when P95×2 > 100% ❌'
        WHEN s.Action = 'NoChange'
        AND  r.CpuMaxP95x2 < 100
        AND  r.CpuAvgP95x2 < 100
        THEN 'STL blocking safe Downsize ⚠️'
        WHEN s.Action = 'Upsize'
        THEN 'STL Upsizing — check if correct'
        ELSE 'Other difference'
    END                                 AS Reason,
    COUNT(DISTINCT s.ClusterKey)        AS UniqueClusters,
    COUNT(*)                            AS TotalSlices
FROM [Metrics].[MongoDBRightsizingRecommendations_STL] s
JOIN (
    SELECT
        ClusterKey,
        [type]                          AS DayType,
        businessHour                    AS HourType,
        FORMAT(_date,'yyyy-MM')         AS Month,
        ROUND(MAX(CpuMaxP95)*2,2)      AS CpuMaxP95x2,
        ROUND(MAX(CpuAvgP95)*2,2)      AS CpuAvgP95x2
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
AND   s.Action != CASE
    WHEN r.CpuMaxP95x2 < 100
    AND  r.CpuAvgP95x2 < 100
    THEN 'Downsize' ELSE 'NoChange' END
GROUP BY
    CASE
        WHEN s.Action = 'Downsize'
        AND  r.CpuMaxP95x2 >= 100
        THEN 'STL Downsizing when P95×2 > 100% ❌'
        WHEN s.Action = 'NoChange'
        AND  r.CpuMaxP95x2 < 100
        AND  r.CpuAvgP95x2 < 100
        THEN 'STL blocking safe Downsize ⚠️'
        WHEN s.Action = 'Upsize'
        THEN 'STL Upsizing — check if correct'
        ELSE 'Other difference'
    END
ORDER BY TotalSlices DESC
GO