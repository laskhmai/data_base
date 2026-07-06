-- =============================================
-- AUTO COMPARISON: STL vs Normal vs Expected
-- Automatically identifies which is correct
-- =============================================

WITH RawP95 AS (
    -- Get CpuMaxP95 from aggregated table
    -- This is the key metric that drives decisions
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
        ROUND(AVG(CpuAvg),    2)            AS AvgCpuAvg,
        ROUND(MAX(ConnUtilizationPct), 2)   AS MaxConnUtil
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
    GROUP BY
        ClusterKey,
        [type],
        businessHour,
        FORMAT(_date,'yyyy-MM')
),
Comparison AS (
    SELECT
        n.ClusterName,
        n.CurrentSku,
        n.DayType,
        n.HourType,
        n.Action                            AS Normal_Action,
        s.Action                            AS STL_Action,
        n.RecommendedSku                    AS Normal_RecommendedSku,
        s.RecommendedSku                    AS STL_RecommendedSku,
        r.CpuMaxP95,
        r.CpuMaxP95x2,
        r.CpuAvgP95,
        r.CpuAvgP95x2,
        r.PeakCpuMax,
        r.AvgCpuAvg,
        r.MaxConnUtil,
        -- Expected action based on our threshold rule
        CASE
            WHEN r.CpuMaxP95x2 < 100
            AND  r.CpuAvgP95x2 < 100
            THEN 'Downsize'
            WHEN r.CpuMaxP95x2 >= 100
            OR   r.CpuAvgP95x2 >= 100
            THEN 'NoChange'
            ELSE 'Unknown'
        END                                 AS Expected_Action
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
    WHERE n.Action != s.Action  -- only different ones
)
-- MAIN RESULT: Full comparison with verdict
SELECT
    ClusterName,
    CurrentSku,
    DayType,
    HourType,
    Normal_Action,
    STL_Action,
    Expected_Action,
    CpuMaxP95,
    CpuMaxP95x2,
    CpuAvgP95,
    CpuAvgP95x2,
    PeakCpuMax,
    AvgCpuAvg,
    MaxConnUtil,
    -- Automated verdict
    CASE
        WHEN Normal_Action = Expected_Action
        AND  STL_Action   != Expected_Action
        THEN 'Normal Correct ✅'

        WHEN STL_Action    = Expected_Action
        AND  Normal_Action != Expected_Action
        THEN 'STL Correct ✅'

        WHEN Normal_Action = Expected_Action
        AND  STL_Action    = Expected_Action
        THEN 'Both Correct ✅'

        ELSE 'Edge Case — Manual Review ⚠️'
    END                                     AS Verdict,

    -- Why different?
    CASE
        WHEN STL_Action = 'Downsize'
        AND  Normal_Action = 'NoChange'
        THEN 'STL identified spike as seasonal — allowed downsize'

        WHEN STL_Action = 'NoChange'
        AND  Normal_Action = 'Downsize'
        THEN 'STL detected real trend — blocked downsize'

        WHEN STL_Action = 'Upsize'
        AND  Normal_Action = 'NoChange'
        THEN 'STL detected stronger trend — recommended upsize'

        ELSE 'Other difference'
    END                                     AS Reason
FROM Comparison
ORDER BY
    CASE
        WHEN Normal_Action = Expected_Action
        AND  STL_Action   != Expected_Action
        THEN 1  -- Normal correct first
        WHEN STL_Action    = Expected_Action
        AND  Normal_Action != Expected_Action
        THEN 2  -- STL correct second
        ELSE 3  -- edge cases last
    END,
    ClusterName
GO

-- SUMMARY
SELECT
    Verdict,
    COUNT(*)    AS ClusterSlices,
    COUNT(DISTINCT ClusterName) AS UniqueClusters
FROM (
    SELECT
        ClusterName,
        CASE
            WHEN n.Action = CASE
                WHEN r.CpuMaxP95x2 < 100 AND r.CpuAvgP95x2 < 100
                THEN 'Downsize' ELSE 'NoChange' END
            AND  s.Action != CASE
                WHEN r.CpuMaxP95x2 < 100 AND r.CpuAvgP95x2 < 100
                THEN 'Downsize' ELSE 'NoChange' END
            THEN 'Normal Correct ✅'
            WHEN s.Action = CASE
                WHEN r.CpuMaxP95x2 < 100 AND r.CpuAvgP95x2 < 100
                THEN 'Downsize' ELSE 'NoChange' END
            AND  n.Action != CASE
                WHEN r.CpuMaxP95x2 < 100 AND r.CpuAvgP95x2 < 100
                THEN 'Downsize' ELSE 'NoChange' END
            THEN 'STL Correct ✅'
            ELSE 'Edge Case ⚠️'
        END                         AS Verdict
    FROM [Metrics].[MongoDBRightsizingRecommendations] n
    JOIN [Metrics].[MongoDBRightsizingRecommendations_STL] s
        ON  s.ClusterKey = n.ClusterKey
        AND s.DayType    = n.DayType
        AND s.HourType   = n.HourType
        AND s.Month      = n.Month
    JOIN (
        SELECT ClusterKey, [type] AS DayType,
               businessHour AS HourType,
               FORMAT(_date,'yyyy-MM') AS Month,
               ROUND(MAX(CpuMaxP95)*2,2) AS CpuMaxP95x2,
               ROUND(MAX(CpuAvgP95)*2,2) AS CpuAvgP95x2
        FROM [Metrics].[MongoDBRightsizingAggregated5Min]
        WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
        GROUP BY ClusterKey,[type],businessHour,
                 FORMAT(_date,'yyyy-MM')
    ) r ON r.ClusterKey=n.ClusterKey
        AND r.DayType=n.DayType
        AND r.HourType=n.HourType
        AND r.Month=n.Month
    WHERE n.Action != s.Action
) x
GROUP BY Verdict
ORDER BY ClusterSlices DESC
GO