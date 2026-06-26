DECLARE @ClusterKey INT = (
    SELECT TOP 1 ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    WHERE ClusterName = 'cmsonc-eob-prod-cluster'
)

-- Compare May vs June values per process
SELECT
    FORMAT(c.DateTime,'yyyy-MM')    AS Month,
    c.[Key]                         AS ProcessId,
    ROUND(AVG(c.Measurement),  2)   AS CpuAvg,
    ROUND(MAX(c.Measurement),  2)   AS CpuMax,
    COUNT(*)                        AS Readings
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M] c
JOIN [MongoDB].[Process] p
    ON  p.ProcessId  = c.[Key]
    AND p.ClusterKey = @ClusterKey
    AND p.IsDeleted  = 0
WHERE FORMAT(c.DateTime,'yyyy-MM') IN ('2026-05','2026-06')
GROUP BY
    FORMAT(c.DateTime,'yyyy-MM'),
    c.[Key]
ORDER BY
    FORMAT(c.DateTime,'yyyy-MM'),
    CpuAvg DESC
GO