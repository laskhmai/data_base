-- ============================================================
-- MongoDB Rightsizing Recommendations — Validation Queries
-- Run after notebook completes
-- ============================================================


-- ============================================================
-- STEP 1: Basic Count Check
-- ============================================================
SELECT
    COUNT(*)                        AS TotalRows,
    COUNT(DISTINCT ClusterKey)      AS UniqueClusters,
    COUNT(DISTINCT DayType)         AS DayTypes,
    COUNT(DISTINCT HourType)        AS HourTypes,
    MIN(AuditUtc)                   AS EarliestRun,
    MAX(AuditUtc)                   AS LatestRun
FROM [Metrics].[MongoDBRightsizingRecommendations]
GO


-- ============================================================
-- STEP 2: Breakdown by Action
-- ============================================================
SELECT
    Action,
    COUNT(*)                        AS TotalRows,
    COUNT(DISTINCT ClusterKey)      AS UniqueClusters
FROM [Metrics].[MongoDBRightsizingRecommendations]
GROUP BY Action
ORDER BY TotalRows DESC
GO


-- ============================================================
-- STEP 3: Breakdown by DayType + HourType
-- Expected: 3 slices per cluster
-- ============================================================
SELECT
    DayType,
    HourType,
    COUNT(*)                        AS TotalRows,
    COUNT(DISTINCT ClusterKey)      AS UniqueClusters
FROM [Metrics].[MongoDBRightsizingRecommendations]
GROUP BY DayType, HourType
ORDER BY DayType, HourType
GO


-- ============================================================
-- STEP 4: Clusters with different recommendations
--         across time slices
-- ============================================================
SELECT
    ClusterKey,
    ClusterName,
    CurrentSku,
    COUNT(DISTINCT RecommendedSku)  AS DifferentRecommendations,
    STRING_AGG(
        HourType + ':' + RecommendedSku, ' | '
    )                               AS RecommendationBySlice
FROM [Metrics].[MongoDBRightsizingRecommendations]
GROUP BY ClusterKey, ClusterName, CurrentSku
HAVING COUNT(DISTINCT RecommendedSku) > 1
ORDER BY DifferentRecommendations DESC
GO


-- ============================================================
-- STEP 5: Validate ScaleDown — Cross-check with raw data
--         Clusters recommended ScaleDown should have
--         LOW CPU, LOW memory, LOW connections
-- ============================================================
SELECT
    r.ClusterName,
    r.CurrentSku,
    r.RecommendedSku,
    r.DayType,
    r.HourType,
    r.Comment,
    -- From aggregated table
    ROUND(MAX(a.CpuAvg),    2)      AS AvgCpu,
    ROUND(MAX(a.CpuMax),    2)      AS MaxCpu,
    ROUND(MAX(a.MemResidentAvgPct), 2) AS AvgMemPct,
    ROUND(MAX(a.MemResidentMaxPct), 2) AS MaxMemPct,
    ROUND(MAX(a.ConnUtilizationPct),2) AS MaxConnPct
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min]  a
    ON  a.ClusterKey = r.ClusterKey
WHERE r.Action  = 'Downsize'
AND   r.DayType = 'Weekday'
AND   r.HourType = 'BusinessHours'
GROUP BY
    r.ClusterName, r.CurrentSku, r.RecommendedSku,
    r.DayType, r.HourType, r.Comment
ORDER BY AvgCpu ASC
GO


-- ============================================================
-- STEP 6: Validate ScaleUp — Cross-check with raw data
--         Clusters recommended ScaleUp should have
--         HIGH CPU or HIGH memory or HIGH connections
-- ============================================================
SELECT
    r.ClusterName,
    r.CurrentSku,
    r.RecommendedSku,
    r.DayType,
    r.HourType,
    r.Comment,
    ROUND(MAX(a.CpuMax),            2) AS MaxCpu,
    ROUND(MAX(a.MemResidentMaxPct), 2) AS MaxMemPct,
    ROUND(MAX(a.ConnUtilizationPct),2) AS MaxConnPct
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min]  a
    ON  a.ClusterKey = r.ClusterKey
WHERE r.Action  = 'Upsize'
AND   r.DayType = 'Weekday'
AND   r.HourType = 'BusinessHours'
GROUP BY
    r.ClusterName, r.CurrentSku, r.RecommendedSku,
    r.DayType, r.HourType, r.Comment
ORDER BY MaxCpu DESC
GO


-- ============================================================
-- STEP 7: Validate NoChange — Should be in normal range
--         CPU 25-50%, Memory 30-70%, Connections 35-80%
-- ============================================================
SELECT
    r.ClusterName,
    r.CurrentSku,
    r.DayType,
    r.HourType,
    ROUND(MAX(a.CpuAvg),            2) AS AvgCpu,
    ROUND(MAX(a.CpuMax),            2) AS MaxCpu,
    ROUND(MAX(a.MemResidentAvgPct), 2) AS AvgMemPct,
    ROUND(MAX(a.MemResidentMaxPct), 2) AS MaxMemPct,
    ROUND(MAX(a.ConnUtilizationPct),2) AS MaxConnPct
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min]  a
    ON  a.ClusterKey = r.ClusterKey
WHERE r.Action  = 'NoChange'
AND   r.DayType = 'Weekday'
AND   r.HourType = 'BusinessHours'
GROUP BY
    r.ClusterName, r.CurrentSku,
    r.DayType, r.HourType
ORDER BY MaxCpu DESC
GO


-- ============================================================
-- STEP 8: Check Known Clusters
--         cdr-uat           → expect Downsize (low usage)
--         cwih-cp-mgmt-prod → expect Upsize   (memory > 100%)
--         consumer-interops → expect Upsize   (conn > 100%)
-- ============================================================
SELECT
    r.ClusterName,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action,
    r.DayType,
    r.HourType,
    r.Comment,
    r.EstimatedMonthlySavings
FROM [Metrics].[MongoDBRightsizingRecommendations] r
WHERE r.ClusterName IN (
    'cdr-uat',
    'cwih-cp-mgmt-prod',
    'consumer-interops-uat'
)
ORDER BY r.ClusterName, r.DayType, r.HourType
GO


-- ============================================================
-- STEP 9: Cost Savings Summary
-- ============================================================
SELECT
    Action,
    COUNT(DISTINCT ClusterKey)              AS Clusters,
    ROUND(SUM(EstimatedMonthlySavings), 2)  AS TotalMonthlySavings,
    ROUND(AVG(EstimatedMonthlySavings), 2)  AS AvgMonthlySavings,
    ROUND(SUM(Spend30days), 2)              AS TotalCurrentSpend
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE DayType  = 'Weekday'
AND   HourType = 'BusinessHours'
GROUP BY Action
ORDER BY TotalMonthlySavings DESC
GO


-- ============================================================
-- STEP 10: Check Efficiency Columns Populated
-- ============================================================
SELECT
    COUNT(*)                            AS TotalRows,
    SUM(CASE WHEN CurrentEfficiency IS NOT NULL
             THEN 1 ELSE 0 END)         AS CurrentEfficiencyPopulated,
    SUM(CASE WHEN WithinEfficiency  IS NOT NULL
             THEN 1 ELSE 0 END)         AS WithinEfficiencyPopulated,
    SUM(CASE WHEN OutsideEfficiency IS NULL
             THEN 1 ELSE 0 END)         AS OutsideEfficiencyNull
FROM [Metrics].[MongoDBRightsizingRecommendations]
GO


-- ============================================================
-- STEP 11: Wrong Recommendations Check
--          Flag suspicious rows where:
--          - ScaleUp but CPU/Mem/Conn all LOW
--          - ScaleDown but CPU or Mem HIGH
-- ============================================================
SELECT
    r.ClusterName,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action,
    r.Comment,
    ROUND(MAX(a.CpuMax),            2) AS MaxCpu,
    ROUND(MAX(a.MemResidentMaxPct), 2) AS MaxMemPct,
    ROUND(MAX(a.ConnUtilizationPct),2) AS MaxConnPct,
    'CHECK: Upsize but all metrics low' AS Flag
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min]  a
    ON  a.ClusterKey = r.ClusterKey
WHERE r.Action   = 'Upsize'
AND   r.DayType  = 'Weekday'
AND   r.HourType = 'BusinessHours'
GROUP BY
    r.ClusterName, r.CurrentSku, r.RecommendedSku,
    r.Action, r.Comment
HAVING MAX(a.CpuMax)            < 25
AND    MAX(a.MemResidentMaxPct) < 50
AND    MAX(a.ConnUtilizationPct)< 35

UNION ALL

SELECT
    r.ClusterName,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action,
    r.Comment,
    ROUND(MAX(a.CpuMax),            2),
    ROUND(MAX(a.MemResidentMaxPct), 2),
    ROUND(MAX(a.ConnUtilizationPct),2),
    'CHECK: Downsize but metrics high'
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min]  a
    ON  a.ClusterKey = r.ClusterKey
WHERE r.Action   = 'Downsize'
AND   r.DayType  = 'Weekday'
AND   r.HourType = 'BusinessHours'
GROUP BY
    r.ClusterName, r.CurrentSku, r.RecommendedSku,
    r.Action, r.Comment
HAVING MAX(a.CpuMax) > 50
    OR MAX(a.MemResidentMaxPct) > 70
ORDER BY Flag, ClusterName
GO