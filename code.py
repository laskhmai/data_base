SELECT
    ROUND(AVG(c.Measurement), 2) AS ExactCpuAvg
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M] c
JOIN [MongoDB].[Process] p
    ON  p.ProcessId  = c.[Key]
    AND p.ClusterKey = 330
WHERE CAST(c.DateTime AS DATE)   = '2026-06-16'
AND   DATEPART(HOUR,c.DateTime)  = 0
