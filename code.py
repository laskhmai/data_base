-- Updated comparison: STL vs Normal after all changes
WITH RawP95 AS (
    SELECT
        ClusterKey,
        [type]                              AS DayType,
        businessHour                        AS HourType,
        FORMAT(_date,'yyyy-MM')             AS Month,
        ROUND(MAX(CpuMaxP95), 2)            AS CpuMaxP95,
        ROUND(MAX(CpuMaxP95) * 2, 2)        AS CpuMaxP95x2,
        ROUND(MAX(CpuAvgP95), 2)            AS CpuAvgP95,
        ROUND(MAX(CpuAvgP95) * 2, 2)        AS CpuAvgP95x2,
        ROUND(MAX(CpuMax),    2)            AS PeakCpuMax,
        ROUND(AVG(CpuAvg),    2)            AS AvgCpuAvg
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
    GROUP BY
        ClusterKey,
        [type],
        businessHour,
        FORMAT(_date,'yyyy-MM')
)
SELECT
    n.ClusterName,
    n.CurrentSku,
    n.DayType,
    n.HourType,
    n.Action                            AS Normal_Action,
    s.Action                            AS STL_Action,
    n.RecommendedSku                    AS Normal_Rec,
    s.RecommendedSku                    AS STL_Rec,
    r.CpuMaxP95,
    r.CpuMaxP95x2,
    r.CpuAvgP95,
    r.PeakCpuMax,
    -- Expected based on threshold rule
    CASE
        WHEN r.CpuMaxP95x2 < 100
        AND  r.CpuAvgP95x2 < 100
        THEN 'Downsize'
        ELSE 'NoChange'
    END                                 AS Expected_Action,
    -- Verdict
    CASE
        WHEN n.Action = s.Action
        THEN 'Both Same ✅'
        WHEN s.Action = CASE
            WHEN r.CpuMaxP95x2 < 100
            AND  r.CpuAvgP95x2 < 100
            THEN 'Downsize' ELSE 'NoChange' END
        AND  n.Action != CASE
            WHEN r.CpuMaxP95x2 < 100
            AND  r.CpuAvgP95x2 < 100
            THEN 'Downsize' ELSE 'NoChange' END
        THEN 'STL Correct ✅'
        WHEN n.Action = CASE
            WHEN r.CpuMaxP95x2 < 100
            AND  r.CpuAvgP95x2 < 100
            THEN 'Downsize' ELSE 'NoChange' END
        AND  s.Action != CASE
            WHEN r.CpuMaxP95x2 < 100
            AND  r.CpuAvgP95x2 < 100
            THEN 'Downsize' ELSE 'NoChange' END
        THEN 'Normal Correct ✅'
        ELSE 'Edge Case ⚠️'
    END                                 AS Verdict
FROM [Metrics].[MongoDBRightsizingRecommendations] n
JOIN [Metrics].[MongoDBRightsizingRecommendations_STL] s
    ON  s.ClusterKey = n.ClusterKey
    AND s.DayType    = n.DayType
    AND s.HourType   = n.HourType
    AND s.Month      = n.Month
JOIN RawP95 r
    ON  r.ClusterKey = n.ClusterKey
    AND r.DayType    = n.DayType
    AND r.HourType   = n.HourType
    AND r.Month      = n.Month
ORDER BY
    CASE WHEN n.Action = s.Action THEN 2 ELSE 1 END,
    n.ClusterName
GO

-- Summary
SELECT
    CASE
        WHEN n.Action = s.Action
        THEN 'Both Same ✅'
        WHEN s.Action = CASE
            WHEN r.CpuMaxP95x2 < 100
            AND  r.CpuAvgP95x2 < 100
            THEN 'Downsize' ELSE 'NoChange' END
        AND  n.Action != CASE
            WHEN r.CpuMaxP95x2 < 100
            AND  r.CpuAvgP95x2 < 100
            THEN 'Downsize' ELSE 'NoChange' END
        THEN 'STL Correct ✅'
        WHEN n.Action = CASE
            WHEN r.CpuMaxP95x2 < 100
            AND  r.CpuAvgP95x2 < 100
            THEN 'Downsize' ELSE 'NoChange' END
        AND  s.Action != CASE
            WHEN r.CpuMaxP95x2 < 100
            AND  r.CpuAvgP95x2 < 100
            THEN 'Downsize' ELSE 'NoChange' END
        THEN 'Normal Correct ✅'
        ELSE 'Edge Case ⚠️'
    END                                 AS Verdict,
    COUNT(DISTINCT n.ClusterKey)        AS UniqueClusters,
    COUNT(*)                            AS TotalSlices
FROM [Metrics].[MongoDBRightsizingRecommendations] n
JOIN [Metrics].[MongoDBRightsizingRecommendations_STL] s
    ON  s.ClusterKey = n.ClusterKey
    AND s.DayType    = n.DayType
    AND s.HourType   = n.HourType
    AND s.Month      = n.Month
JOIN (
    SELECT ClusterKey,
           [type] AS DayType,
           businessHour AS HourType,
           FORMAT(_date,'yyyy-MM') AS Month,
           ROUND(MAX(CpuMaxP95)*2,2) AS CpuMaxP95x2,
           ROUND(MAX(CpuAvgP95)*2,2) AS CpuAvgP95x2
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
    GROUP BY ClusterKey,[type],businessHour,
             FORMAT(_date,'yyyy-MM')
) r
    ON  r.ClusterKey = n.ClusterKey
    AND r.DayType    = n.DayType
    AND r.HourType   = n.HourType
    AND r.Month      = n.Month
GROUP BY
    CASE
        WHEN n.Action = s.Action THEN 'Both Same ✅'
        WHEN s.Action = CASE WHEN r.CpuMaxP95x2 < 100
            AND r.CpuAvgP95x2 < 100 THEN 'Downsize'
            ELSE 'NoChange' END
        AND n.Action != CASE WHEN r.CpuMaxP95x2 < 100
            AND r.CpuAvgP95x2 < 100 THEN 'Downsize'
            ELSE 'NoChange' END THEN 'STL Correct ✅'
        WHEN n.Action = CASE WHEN r.CpuMaxP95x2 < 100
            AND r.CpuAvgP95x2 < 100 THEN 'Downsize'
            ELSE 'NoChange' END
        AND s.Action != CASE WHEN r.CpuMaxP95x2 < 100
            AND r.CpuAvgP95x2 < 100 THEN 'Downsize'
            ELSE 'NoChange' END THEN 'Normal Correct ✅'
        ELSE 'Edge Case ⚠️'
    END
ORDER BY TotalSlices DESC
GO