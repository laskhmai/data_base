-- Find top 5 clusters with highest memory %
-- AND verify they have raw source data
SELECT TOP 5
    s.ClusterName,
    s.InstanceSize,
    s.ProviderName,
    MAX(s.MemResidentMax)    AS MaxRawMemMB,
    MAX(s.MemResidentMaxPct) AS MaxMemPct
FROM [Metrics].[MongoDBRightsizingAggregated5Min] s
WHERE s.MemResidentMaxPct > 100
GROUP BY
    s.ClusterName,
    s.InstanceSize,
    s.ProviderName
ORDER BY MAX(s.MemResidentMaxPct) DESC
GO

-- Replace 'CLUSTER_NAME_HERE' with result above
SELECT TOP 10
    Measurement          AS RawMemoryMB,
    DateTime
FROM [Metrics].[MongoDB_Memory_Resident_5M]
WHERE [key] IN (
    SELECT p.ProcessId
    FROM [MongoDB].[Process] p
    JOIN [MongoDB].[Clusters] cl
        ON cl.ClustersKey = p.ClusterKey
    WHERE cl.Name     = 'CLUSTER_NAME_HERE'
    AND   p.IsDeleted = 0
)
AND DateTime >= DATEADD(DAY, -7, GETDATE())
ORDER BY Measurement DESC
GO