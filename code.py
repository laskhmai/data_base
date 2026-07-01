-- =============================================
-- STANDALONE TEST: Verify new P95 calculation
-- Tests that CpuMaxP95 differs from CpuMax
-- and that #TrueClusterP95 has 1 row per hour
-- Run this independently without the proc
-- =============================================

DECLARE @StartDT DATE = '2026-06-01'
DECLARE @EndDT   DATE = '2026-06-30'
DECLARE @ClusterName NVARCHAR(255) = 'cdr-uat'

-- =============================================
-- STEP 1: Get hourly CPU values per process
-- (same as proc does internally)
-- =============================================
;WITH CpuAvgRaw AS (
    SELECT
        p.ClusterKey,
        DATEADD(HOUR, DATEDIFF(HOUR, 0,
            SWITCHOFFSET(CONVERT(datetimeoffset, c.DateTime), '-05:00')), 0)
                                            AS HourBucket,
        AVG(c.Measurement)                  AS CpuAvg
    FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M] c
    JOIN [MongoDB].[Process] p
        ON  p.ProcessId  = c.[Key]
        AND p.IsDeleted  = 0
    WHERE c.DateTime >= @StartDT
    AND   c.DateTime <  @EndDT
    AND   p.ClusterKey IN (
        SELECT ClustersKey FROM [MongoDB].[Clusters]
        WHERE Name = @ClusterName
    )
    GROUP BY p.ClusterKey,
        DATEADD(HOUR, DATEDIFF(HOUR, 0,
            SWITCHOFFSET(CONVERT(datetimeoffset, c.DateTime), '-05:00')), 0)
),
CpuMaxRaw AS (
    SELECT
        p.ClusterKey,
        DATEADD(HOUR, DATEDIFF(HOUR, 0,
            SWITCHOFFSET(CONVERT(datetimeoffset, c.DateTime), '-05:00')), 0)
                                            AS HourBucket,
        MAX(c.Measurement)                  AS CpuMax
    FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_5M] c
    JOIN [MongoDB].[Process] p
        ON  p.ProcessId  = c.[Key]
        AND p.IsDeleted  = 0
    WHERE c.DateTime >= @StartDT
    AND   c.DateTime <  @EndDT
    AND   p.ClusterKey IN (
        SELECT ClustersKey FROM [MongoDB].[Clusters]
        WHERE Name = @ClusterName
    )
    GROUP BY p.ClusterKey,
        DATEADD(HOUR, DATEDIFF(HOUR, 0,
            SWITCHOFFSET(CONVERT(datetimeoffset, c.DateTime), '-05:00')), 0)
),

-- =============================================
-- STEP 2: Cluster-level per hour
-- MAX CpuMax across all processes for that hour
-- =============================================
ClusterHourly AS (
    SELECT
        COALESCE(a.ClusterKey, m.ClusterKey)   AS ClusterKey,
        COALESCE(a.HourBucket, m.HourBucket)   AS HourBucket,
        CAST(COALESCE(a.HourBucket,
             m.HourBucket) AS DATE)             AS _date,
        DATEPART(HOUR, COALESCE(a.HourBucket,
             m.HourBucket))                     AS _hour,
        CASE WHEN DATEPART(WEEKDAY,
             COALESCE(a.HourBucket, m.HourBucket))
             IN (1,7) THEN 'Weekend'
             ELSE 'Weekday' END                 AS [type],
        CASE WHEN DATEPART(WEEKDAY,
             COALESCE(a.HourBucket, m.HourBucket))
             IN (1,7) THEN 'Weekend'
             WHEN DATEPART(HOUR,
             COALESCE(a.HourBucket, m.HourBucket))
             BETWEEN 7 AND 18 THEN 'BusinessHours'
             ELSE 'NonBusinessHours' END        AS businessHour,
        AVG(a.CpuAvg)                          AS CpuAvg,
        MAX(m.CpuMax)                          AS CpuMax
    FROM CpuAvgRaw a
    FULL JOIN CpuMaxRaw m
        ON  m.ClusterKey = a.ClusterKey
        AND m.HourBucket = a.HourBucket
    GROUP BY
        COALESCE(a.ClusterKey, m.ClusterKey),
        COALESCE(a.HourBucket, m.HourBucket)
),

-- =============================================
-- STEP 3: Rank HOURS for temporal P95
-- =============================================
RankedHours AS (
    SELECT
        ClusterKey, _date, _hour,
        [type], businessHour,
        CpuAvg, CpuMax,
        ROW_NUMBER() OVER (
            PARTITION BY ClusterKey, [type], businessHour
            ORDER BY CpuAvg ASC)               AS CpuAvgRn,
        ROW_NUMBER() OVER (
            PARTITION BY ClusterKey, [type], businessHour
            ORDER BY CpuMax ASC)               AS CpuMaxRn,
        COUNT(*) OVER (
            PARTITION BY ClusterKey, [type], businessHour) AS TotalHours
    FROM ClusterHourly
),

-- =============================================
-- STEP 4: Pick value at 95th percentile hour
-- =============================================
P95Values AS (
    SELECT
        ClusterKey, [type], businessHour,
        TotalHours,
        MAX(CASE WHEN CpuAvgRn = CAST(CEILING(TotalHours * 0.95) AS INT)
                 THEN CpuAvg END)              AS CpuAvgP95,
        MAX(CASE WHEN CpuMaxRn = CAST(CEILING(TotalHours * 0.95) AS INT)
                 THEN CpuMax END)              AS CpuMaxP95
    FROM RankedHours
    GROUP BY ClusterKey, [type], businessHour, TotalHours
)

-- =============================================
-- FINAL: Show comparison
-- CpuMaxP95 should be LESS than CpuMax now
-- =============================================
SELECT
    ch._date,
    ch._hour,
    ch.[type]                               AS DayType,
    ch.businessHour                         AS HourType,
    ROUND(ch.CpuMax,    2)                  AS CpuMax,
    ROUND(p.CpuMaxP95,  2)                  AS CpuMaxP95_NEW,
    ROUND(ch.CpuAvg,    2)                  AS CpuAvg,
    ROUND(p.CpuAvgP95,  2)                  AS CpuAvgP95_NEW,
    p.TotalHours,
    CASE
        WHEN ROUND(p.CpuMaxP95, 2) = ROUND(ch.CpuMax, 2)
        THEN 'Same as MAX ❌'
        ELSE 'True P95 ✅'
    END                                     AS P95Check
FROM ClusterHourly ch
JOIN P95Values p
    ON  p.ClusterKey   = ch.ClusterKey
    AND p.[type]       = ch.[type]
    AND p.businessHour = ch.businessHour
ORDER BY ch._date, ch._hour, ch.businessHour
GO