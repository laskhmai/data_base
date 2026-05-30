-- Check actual raw memory values from source table
SELECT TOP 20
    p.ProcessType,
    p.ProcessId,
    MAX(cn.Measurement) AS MaxRawMemoryMB,
    AVG(cn.Measurement) AS AvgRawMemoryMB,
    -- What % is this of M50 RAM (32GB)?
    ROUND((MAX(cn.Measurement) / (32 * 1024.0)) * 100, 2) AS MemPctOfM50
FROM [Metrics].[MongoDB_Memory_Resident_5M] cn
JOIN [MongoDB].[Process] p
    ON p.ProcessId = cn.[key]
WHERE p.ClusterKey = 80
AND   p.IsDeleted  = 0
AND   cn.DateTime >= DATEADD(DAY, -7, GETDATE())
GROUP BY p.ProcessType, p.ProcessId
ORDER BY MaxRawMemoryMB DESC
GO