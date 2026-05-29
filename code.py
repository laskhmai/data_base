-- Check raw memory values for a cluster showing > 100%
-- Pick one cluster from Step 3 results e.g. aaa-appld-dkt3-uat
SELECT
    [key],
    DateTime,
    Measurement          AS RawMemoryMB,
    -- What percentage would this be for M40 (16GB)?
    ROUND((Measurement / (16 * 1024.0)) * 100, 2) AS MemPctIfM40
FROM [Metrics].[MongoDB_Memory_Resident_5M]
WHERE [key] IN (
    SELECT p.ProcessId
    FROM [MongoDB].[Process] p
    JOIN [MongoDB].[Clusters] cl ON cl.ClustersKey = p.ClusterKey
    WHERE cl.Name   = 'aaa-appld-dkt3-uat'
    AND   p.IsDeleted = 0
)
AND DateTime >= DATEADD(DAY, -1, GETDATE())
ORDER BY Measurement DESC
GO