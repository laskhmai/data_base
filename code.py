-- Check if IOPS is in ReplicationSpecs JSON
SELECT TOP 5
    Name,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].electableSpecs.instanceSize') AS InstanceSize,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].electableSpecs.diskIOPS')     AS DiskIOPS,
    JSON_VALUE(ReplicationSpecs,
        '$[0].regionConfigs[0].electableSpecs.ebsVolumeType') AS VolumeType
FROM [MongoDB].[Clusters]
WHERE StateName = 'IDLE'
AND   Paused    = 0
GO

-- What IOPS values do our clusters actually hit?
SELECT
    p.ClusterKey,
    cl.Name                         AS ClusterName,
    ROUND(AVG(r.Measurement), 2)    AS AvgReadIOPS,
    ROUND(MAX(r.Measurement), 2)    AS MaxReadIOPS
FROM [Metrics].[MongoDB_Disk_partition_Iops_Read_1H] r
JOIN [MongoDB].[Process] p
    ON p.ProcessId = r.[Key]
    AND p.IsDeleted = 0
JOIN [MongoDB].[Clusters] cl
    ON cl.ClustersKey = p.ClusterKey
WHERE FORMAT(r.DateTime,'yyyy-MM') = '2026-06'
GROUP BY p.ClusterKey, cl.Name
ORDER BY MaxReadIOPS DESC
GO