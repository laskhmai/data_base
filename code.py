-- =============================================
-- VALIDATION QUERY FOR NEERAJA
-- Shows raw per-process metrics + recommendation
-- For 3 sample clusters
-- =============================================

DECLARE @Month CHAR(7) = '2026-06'

-- PART 1: Our recommendations for 3 clusters
SELECT
    r.ClusterName,
    r.DayType,
    r.HourType,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action,
    ROUND(r.AvgCpuMax,   2)             AS AvgCpuMax,
    ROUND(r.PeakCpuMax,  2)             AS PeakCpuMax,
    ROUND(MAX(a.CpuMaxP95),    2)       AS CpuMaxP95,
    ROUND(MAX(a.CpuMaxP95)*2, 2)        AS CpuMaxP95x2,
    ROUND(r.EstimatedMonthlySavings, 2) AS Savings,
    r.Comment
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = r.ClusterKey
    AND a.[type]       = r.DayType
    AND a.businessHour = r.HourType
    AND FORMAT(a._date,'yyyy-MM') = @Month
WHERE r.Month    = @Month
AND   r.DayType  = 'Weekday'
AND   r.HourType = 'BusinessHours'
AND   r.ClusterName IN (
    'cdr-uat',
    'cwih-cp-mgmt-prod',
    'cmsonc-eob-prod-cluster'
)
GROUP BY
    r.ClusterName, r.DayType, r.HourType,
    r.CurrentSku, r.RecommendedSku, r.Action,
    r.AvgCpuMax, r.PeakCpuMax,
    r.EstimatedMonthlySavings, r.Comment
ORDER BY r.ClusterName
GO

-- PART 2: Raw per-process validation
-- Same clusters, same month
-- Neeraja can verify our numbers against raw
DECLARE @Month CHAR(7) = '2026-06'

SELECT
    cl.Name                             AS ClusterName,
    p.ProcessId,
    p.ProcessType,
    ROUND(AVG(c.Measurement),   2)      AS CpuAvg,
    ROUND(MAX(cm.Measurement),  2)      AS CpuMax,
    -- P95 across time for this process
    ROUND(
        MAX(CASE
            WHEN rn = CAST(CEILING(cnt * 0.95) AS INT)
            THEN cm.Measurement
        END), 2)                        AS CpuMaxP95,
    COUNT(DISTINCT CAST(c.DateTime AS DATE)) AS DaysOfData
FROM [MongoDB].[Clusters] cl
JOIN [MongoDB].[Process] p
    ON  p.ClusterKey = cl.ClustersKey
    AND p.IsDeleted  = 0
JOIN [Metrics].[MongoDB_System_Normalized_Cpu_User_5M] c
    ON  c.[Key]      = p.ProcessId
JOIN (
    -- Per process CpuMax with row number for P95
    SELECT
        [Key],
        Measurement,
        ROW_NUMBER() OVER (
            PARTITION BY [Key]
            ORDER BY Measurement ASC)   AS rn,
        COUNT(*) OVER (
            PARTITION BY [Key])         AS cnt
    FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_5M]
    WHERE FORMAT(DateTime,'yyyy-MM') = @Month
    AND   DATEPART(WEEKDAY, DateTime) NOT IN (1,7)
    AND   DATEPART(HOUR, DateTime) BETWEEN 7 AND 18
) cm ON cm.[Key] = p.ProcessId
WHERE cl.Name IN (
    'cdr-uat',
    'cwih-cp-mgmt-prod',
    'cmsonc-eob-prod-cluster'
)
AND FORMAT(c.DateTime,'yyyy-MM') = @Month
AND DATEPART(WEEKDAY, c.DateTime) NOT IN (1,7)
AND DATEPART(HOUR,    c.DateTime) BETWEEN 7 AND 18
GROUP BY
    cl.Name,
    p.ProcessId,
    p.ProcessType
ORDER BY
    cl.Name,
    CASE p.ProcessType
        WHEN 'REPLICA_PRIMARY'   THEN 1
        WHEN 'REPLICA_SECONDARY' THEN 2
        ELSE 3
    END,
    CpuMax DESC
GO