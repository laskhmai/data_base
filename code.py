-- Get exactly 3 rows (one per process)
DECLARE @ClusterKey  INT    = (
    SELECT TOP 1 ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    WHERE ClusterName = 'cmsonc-eob-prod-cluster'
)
DECLARE @Month CHAR(7) = '2026-05'

;WITH CpuPerProcess AS (
    SELECT
        c.[Key]             AS ProcessId,
        AVG(c.Measurement)  AS CpuAvg,
        MAX(c.Measurement)  AS CpuMax,
        COUNT(*)            AS Readings
    FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M] c
    JOIN [MongoDB].[Process] p
        ON  p.ProcessId  = c.[Key]
        AND p.ClusterKey = @ClusterKey
        AND p.IsDeleted  = 0
    WHERE FORMAT(c.DateTime,'yyyy-MM') = @Month
    GROUP BY c.[Key]
),
MaxPerProcess AS (
    SELECT
        cm.[Key]            AS ProcessId,
        MAX(cm.Measurement) AS CpuMaxFromMaxTable
    FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_5M] cm
    JOIN [MongoDB].[Process] p
        ON  p.ProcessId  = cm.[Key]
        AND p.ClusterKey = @ClusterKey
        AND p.IsDeleted  = 0
    WHERE FORMAT(cm.DateTime,'yyyy-MM') = @Month
    GROUP BY cm.[Key]
)
SELECT
    r.ClusterName,
    cpu.ProcessId,
    p.ProcessType,
    ROUND(cpu.CpuAvg, 2)            AS CpuAvg,
    ROUND(mx.CpuMaxFromMaxTable, 2) AS CpuMax,
    r.CurrentSku,
    r.RecommendedSku,
    r.Action
FROM CpuPerProcess cpu
JOIN [MongoDB].[Process] p
    ON  p.ProcessId  = cpu.ProcessId
    AND p.ClusterKey = @ClusterKey
    AND p.IsDeleted  = 0
JOIN MaxPerProcess mx
    ON  mx.ProcessId = cpu.ProcessId
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