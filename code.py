-- =============================================
-- VALIDATION 1: Table row counts and date range
-- =============================================
SELECT
    'Aggregated'                        AS TableName,
    COUNT(*)                            AS TotalRows,
    COUNT(DISTINCT ClusterKey)          AS Clusters,
    MIN(_date)                          AS DataFrom,
    MAX(_date)                          AS DataTo
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE FORMAT(_date,'yyyy-MM') = '2026-05'
UNION ALL
SELECT
    'Recommendations',
    COUNT(*),
    COUNT(DISTINCT ClusterKey),
    NULL, NULL
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month = '2026-05'
UNION ALL
SELECT
    'SimulatedMetrics',
    COUNT(*),
    COUNT(DISTINCT ClusterKey),
    MIN([Date]),
    MAX([Date])
FROM [Metrics].[MongoDBRightsizingSimulatedMetrics]
WHERE FORMAT(CAST([Date] AS DATE),'yyyy-MM') = '2026-05'
GO

-- =============================================
-- VALIDATION 2: No duplicates in Recommendations
-- =============================================
SELECT
    COUNT(*)                            AS TotalRows,
    COUNT(DISTINCT CONCAT(
        Month,
        CAST(ClusterKey AS VARCHAR),
        DayType,
        HourType
    ))                                  AS UniqueRows,
    COUNT(*) - COUNT(DISTINCT CONCAT(
        Month,
        CAST(ClusterKey AS VARCHAR),
        DayType,
        HourType
    ))                                  AS DuplicateRows
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month = '2026-05'
GO

-- =============================================
-- VALIDATION 3: No duplicates in SimulatedMetrics
-- =============================================
SELECT
    COUNT(*)                            AS TotalRows,
    COUNT(DISTINCT CONCAT(
        CAST(ClusterKey AS VARCHAR),
        CAST([Date] AS VARCHAR),
        CAST([Hour] AS VARCHAR),
        DayType,
        HourType,
        CurrentSku
    ))                                  AS UniqueRows,
    COUNT(*) - COUNT(DISTINCT CONCAT(
        CAST(ClusterKey AS VARCHAR),
        CAST([Date] AS VARCHAR),
        CAST([Hour] AS VARCHAR),
        DayType,
        HourType,
        CurrentSku
    ))                                  AS DuplicateRows
FROM [Metrics].[MongoDBRightsizingSimulatedMetrics]
WHERE FORMAT(CAST([Date] AS DATE),'yyyy-MM') = '2026-05'
GO

-- =============================================
-- VALIDATION 4: Date range check
-- Must be May 1 to May 31 (full previous month)
-- NOT May 17 to May 31 (old bug)
-- =============================================
SELECT
    MIN(_date)              AS DataFrom,
    MAX(_date)              AS DataTo,
    COUNT(DISTINCT _date)   AS UniqueDays,
    CASE
        WHEN MIN(_date) = '2026-05-01'
        THEN 'CORRECT - Full May'
        ELSE 'WRONG - Partial May'
    END                     AS DateRangeCheck
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE FORMAT(_date,'yyyy-MM') = '2026-05'
GO

-- =============================================
-- VALIDATION 5: Cluster inventory source check
-- Verify active clusters from MongoDB.Clusters
-- =============================================
SELECT
    'In Recommendations'        AS Source,
    COUNT(DISTINCT ClusterKey)  AS Clusters
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month = '2026-05'
UNION ALL
SELECT
    'Active in MongoDB.Clusters',
    COUNT(DISTINCT ClustersKey)
FROM [MongoDB].[Clusters]
WHERE StateName IN ('IDLE','UPDATING')
AND   Paused = 0
GO

-- =============================================
-- VALIDATION 6: Action distribution
-- =============================================
SELECT
    Month,
    Action,
    COUNT(DISTINCT ClusterKey)          AS Clusters,
    ROUND(SUM(Spend30days),2)           AS TotalSpend,
    ROUND(SUM(EstimatedMonthlySavings),2) AS TotalSavings
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month    = '2026-05'
AND   DayType  = 'Weekday'
AND   HourType = 'BusinessHours'
GROUP BY Month, Action
ORDER BY Action
GO

-- =============================================
-- VALIDATION 7: Known cluster validation
-- cdr-uat, cwih, consumer-interops-uat
-- consumer-interops-qa (bug fix cluster)
-- =============================================
SELECT
    ClusterName,
    DayType,
    HourType,
    CurrentSku,
    RecommendedSku,
    LowCpuSku,
    Action,
    ROUND(EstimatedMonthlySavings,2)    AS Savings,
    ROUND(Spend30days,2)                AS Spend30days,
    Comment
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month = '2026-05'
AND   ClusterName IN (
    'cdr-uat',
    'cwih-cp-mgmt-prod',
    'consumer-interops-uat',
    'consumer-interops-qa'
)
ORDER BY ClusterName, DayType, HourType
GO

-- =============================================
-- VALIDATION 8: CpuMax spike check
-- No cluster with CpuMax > 60% should be Downsize
-- This was the core bug Neeraja found
-- =============================================
SELECT
    r.ClusterName,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action,
    ROUND(MAX(a.CpuMax),2)              AS MaxCpuSpike,
    ROUND(AVG(a.CpuAvg),2)             AS AvgCpu
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
HAVING MAX(a.CpuMax) > 60
ORDER BY MaxCpuSpike DESC
GO

-- =============================================
-- VALIDATION 9: Memory not driving recommendations
-- MemRec should equal CurrentSku for all clusters
-- Since memory flag is disabled
-- =============================================
SELECT
    COUNT(*)                            AS TotalRows,
    SUM(CASE WHEN MemRec != CurrentSku
             THEN 1 ELSE 0 END)         AS MemDrivingRec,
    SUM(CASE WHEN MemRec = CurrentSku
             THEN 1 ELSE 0 END)         AS MemNotDriving
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month = '2026-05'
GO

-- =============================================
-- VALIDATION 10: Spend from Spend table check
-- Spend30days should match MongoDB.Spend
-- =============================================
SELECT
    r.ClusterName,
    r.DayType,
    r.HourType,
    ROUND(r.Spend30days,2)              AS OurSpend,
    ROUND(s.ActualSpend,2)              AS SpendTableAmount,
    CASE
        WHEN ABS(r.Spend30days -
             COALESCE(s.ActualSpend,0)) < 1
        THEN 'Match'
        ELSE 'Mismatch'
    END                                 AS SpendCheck
FROM [Metrics].[MongoDBRightsizingRecommendations] r
LEFT JOIN (
    SELECT
        Cluster                         AS ClusterName,
        ROUND(SUM(Amount),2)            AS ActualSpend
    FROM [MongoDB].[Spend]
    WHERE FORMAT(CAST(UsageDate AS DATE),'yyyy-MM') = '2026-05'
    GROUP BY Cluster
) s ON s.ClusterName = r.ClusterName
WHERE r.Month    = '2026-05'
AND   r.DayType  = 'Weekday'
AND   r.HourType = 'BusinessHours'
ORDER BY SpendCheck DESC
GO

-- =============================================
-- VALIDATION 11: Efficiency columns populated
-- =============================================
SELECT
    ClusterName,
    DayType,
    HourType,
    CASE WHEN CurrentEfficiency IS NOT NULL
         THEN 'Populated' ELSE 'NULL' END   AS CurrentEff,
    CASE WHEN WithinEfficiency  IS NOT NULL
         THEN 'Populated' ELSE 'NULL' END   AS WithinEff,
    CASE WHEN LowCpuEfficiency  IS NOT NULL
         THEN 'Populated' ELSE 'NULL' END   AS LowCpuEff
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month = '2026-05'
AND   ClusterName IN (
    'cdr-uat',
    'cwih-cp-mgmt-prod',
    'consumer-interops-uat',
    'consumer-interops-qa'
)
ORDER BY ClusterName, DayType, HourType
GO

-- =============================================
-- VALIDATION 12: cmsonc-eob-prod-cluster
-- Neeraja's specific cluster
-- Must NOT be Downsize with high CpuMax
-- =============================================
SELECT
    r.ClusterName,
    r.DayType,
    r.HourType,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action,
    ROUND(MAX(a.CpuMax),2)              AS MaxCpuSpike,
    ROUND(AVG(a.CpuAvg),2)             AS AvgCpu
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey = r.ClusterKey
    AND FORMAT(a._date,'yyyy-MM') = r.Month
WHERE r.Month       = '2026-05'
AND   r.ClusterName = 'cmsonc-eob-prod-cluster'
GROUP BY
    r.ClusterName,
    r.DayType,
    r.HourType,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action
ORDER BY r.DayType, r.HourType
GO

-- =============================================
-- VALIDATION 13: Simulated metrics math check
-- CpuMax ratio must = vCores ratio
-- cdr-uat M60(16) to M50(8) ratio = 2.0
-- =============================================
SELECT TOP 5
    s.ClusterName,
    s.[Date],
    s.[Hour],
    s.CurrentSku,
    ROUND(s.CpuAvg,2)                  AS CpuAvg_Current,
    ROUND(s.nCpuAvgWithin,2)           AS CpuAvg_Projected,
    ROUND(s.nCpuAvgWithin
        / NULLIF(s.CpuAvg,0),2)        AS Ratio,
    CASE
        WHEN ABS(s.nCpuAvgWithin
            / NULLIF(s.CpuAvg,0) - 2.0) < 0.01
        THEN 'Correct'
        ELSE 'Wrong'
    END                                AS MathCheck
FROM [Metrics].[MongoDBRightsizingSimulatedMetrics] s
WHERE s.ClusterName = 'cdr-uat'
AND   s.CpuAvg     > 0
ORDER BY s.[Date] DESC, s.[Hour]
GO

-- =============================================
-- VALIDATION 14: consumer-interops-qa
-- Before: 6 rows, Upsize, negative savings
-- After:  3 rows, NoChange, zero savings
-- =============================================
SELECT
    Month,
    ClusterName,
    DayType,
    HourType,
    Action,
    ROUND(Spend30days,2)               AS Spend30days,
    ROUND(EstimatedMonthlySavings,2)   AS Savings,
    AuditUtc
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE ClusterName = 'consumer-interops-qa'
ORDER BY Month, DayType, HourType
GO

-- =============================================
-- VALIDATION 15: Cost summary
-- =============================================
SELECT
    Month,
    COUNT(DISTINCT ClusterKey)          AS TotalClusters,
    ROUND(SUM(Spend30days),2)           AS TotalCurrentSpend,
    ROUND(SUM(CASE WHEN Action='Downsize'
        THEN EstimatedMonthlySavings
        ELSE 0 END),2)                  AS PotentialSavings,
    ROUND(SUM(CASE WHEN Action='Upsize'
        THEN ABS(EstimatedMonthlySavings)
        ELSE 0 END),2)                  AS AdditionalCost,
    ROUND(SUM(
        EstimatedMonthlySavings),2)     AS NetMonthlySavings,
    ROUND(SUM(
        EstimatedMonthlySavings)*12,2)  AS NetAnnualSavings
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month    = '2026-05'
AND   DayType  = 'Weekday'
AND   HourType = 'BusinessHours'
GROUP BY Month
GO