DECLARE @ClusterKey INT = (
    SELECT TOP 1 ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    WHERE ClusterName = 'cmsonc-eob-prod-cluster'
)
DECLARE @Month CHAR(7) = '2026-05'

;WITH CpuPerProcess AS (
    SELECT
        c.[Key]                     AS ProcessId,
        AVG(c.Measurement)          AS CpuAvg,
        MAX(c.Measurement)          AS CpuMax,
        COUNT(*)                    AS Readings
    FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M] c
    JOIN [MongoDB].[Process] p
        ON  p.ProcessId  = c.[Key]
        AND p.ClusterKey = @ClusterKey
        AND p.IsDeleted  = 0
    WHERE FORMAT(c.DateTime,'yyyy-MM') = @Month
    GROUP BY c.[Key]
),
P99PerProcess AS (
    SELECT DISTINCT
        c.[Key]                     AS ProcessId,
        PERCENTILE_CONT(0.99)
            WITHIN GROUP (ORDER BY c.Measurement)
            OVER (PARTITION BY c.[Key])  AS CpuAvgP99
    FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M] c
    JOIN [MongoDB].[Process] p
        ON  p.ProcessId  = c.[Key]
        AND p.ClusterKey = @ClusterKey
        AND p.IsDeleted  = 0
    WHERE FORMAT(c.DateTime,'yyyy-MM') = @Month
),
MaxPerProcess AS (
    SELECT
        cm.[Key]                    AS ProcessId,
        MAX(cm.Measurement)         AS CpuMax
    FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_5M] cm
    JOIN [MongoDB].[Process] p
        ON  p.ProcessId  = cm.[Key]
        AND p.ClusterKey = @ClusterKey
        AND p.IsDeleted  = 0
    WHERE FORMAT(cm.DateTime,'yyyy-MM') = @Month
    GROUP BY cm.[Key]
),
P95MaxPerProcess AS (
    SELECT DISTINCT
        cm.[Key]                    AS ProcessId,
        PERCENTILE_CONT(0.95)
            WITHIN GROUP (ORDER BY cm.Measurement)
            OVER (PARTITION BY cm.[Key]) AS CpuMaxP95
    FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_5M] cm
    JOIN [MongoDB].[Process] p
        ON  p.ProcessId  = cm.[Key]
        AND p.ClusterKey = @ClusterKey
        AND p.IsDeleted  = 0
    WHERE FORMAT(cm.DateTime,'yyyy-MM') = @Month
)
SELECT
    r.ClusterName,
    cpu.ProcessId,
    p.ProcessType,
    ROUND(cpu.CpuAvg,    2)   AS CpuAvg,
    ROUND(p99.CpuAvgP99, 2)   AS CpuAvgP99,
    ROUND(mx.CpuMax,     2)   AS CpuMax,
    ROUND(p95.CpuMaxP95, 2)   AS CpuMaxP95,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action
FROM CpuPerProcess    cpu
JOIN [MongoDB].[Process] p
    ON  p.ProcessId  = cpu.ProcessId
    AND p.ClusterKey = @ClusterKey
    AND p.IsDeleted  = 0
JOIN MaxPerProcess    mx  ON mx.ProcessId  = cpu.ProcessId
JOIN P99PerProcess    p99 ON p99.ProcessId = cpu.ProcessId
JOIN P95MaxPerProcess p95 ON p95.ProcessId = cpu.ProcessId
LEFT JOIN [Metrics].[MongoDBRightsizingRecommendations] r
    ON  r.ClusterKey = @ClusterKey
    AND r.Month      = @Month
    AND r.DayType    = 'Weekday'
    AND r.HourType   = 'BusinessHours'
ORDER BY
    CASE p.ProcessType
        WHEN 'REPLICA_PRIMARY'   THEN 1
        WHEN 'REPLICA_SECONDARY' THEN 2
        ELSE 3
    END
GO