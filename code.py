-- Step 1: Does this cluster even exist in aggregated table?
SELECT
    ClusterName,
    MemResidentMax,
    MemResidentMaxPct,
    InstanceSize
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName LIKE '%aaa-appld%'
ORDER BY MemResidentMaxPct DESC
GO

-- Step 2: What processes does this cluster have?
SELECT
    p.ProcessId,
    p.ProcessType,
    p.IsDeleted
FROM [MongoDB].[Process] p
JOIN [MongoDB].[Clusters] cl
    ON cl.ClustersKey = p.ClusterKey
WHERE cl.Name = 'aaa-appld-dkt3-uat'
GO

-- Step 3: Does any memory data exist for this cluster at all?
SELECT TOP 5
    [key],
    MAX(Measurement) AS MaxMemory
FROM [Metrics].[MongoDB_Memory_Resident_5M]
WHERE [key] IN (
    SELECT p.ProcessId
    FROM [MongoDB].[Process] p
    JOIN [MongoDB].[Clusters] cl
        ON cl.ClustersKey = p.ClusterKey
    WHERE cl.Name = 'aaa-appld-dkt3-uat'
)
GROUP BY [key]
GO