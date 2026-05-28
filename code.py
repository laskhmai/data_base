SELECT
    [key],
    DATEADD(HOUR, DATEDIFF(HOUR, 0,
        SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                            AS HourBucket,
    MAX(Measurement)        AS ConnectionsMax,
    AVG(Measurement)        AS ConnectionsAvg
FROM [Metrics].[MongoDB_Connections_15M]
WHERE [key] IN (
    SELECT p.ProcessId
    FROM [MongoDB].[Process] p
    JOIN [MongoDB].[Clusters] cl ON cl.ClustersKey = p.ClusterKey
    WHERE cl.Name       = 'cdr-uat'
    AND   p.ProcessType = 'REPLICA_PRIMARY'
    AND   p.IsDeleted   = 0
)
AND DateTime >= '2026-05-21'
AND DateTime <  '2026-05-22'
GROUP BY
    [key],
    DATEADD(HOUR, DATEDIFF(HOUR, 0,
        SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
ORDER BY HourBucket, [key]
GO