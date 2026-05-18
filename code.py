-- Today's records by Organization
SELECT 
    o.Name                          AS OrgName,
    COUNT(DISTINCT p.ClusterKey)    AS Clusters,
    COUNT(DISTINCT h.ProcessId)     AS Processes,
    COUNT(*)                        AS TotalRows,
    MIN(h.DateTimeEST)              AS EarliestHour,
    MAX(h.DateTimeEST)              AS LatestHour
FROM [Metrics].[MongoDBRightsizingAggregatedHourly] h
JOIN [MongoDB].[Process] p  ON p.ProcessId = h.ProcessId
JOIN [MongoDB].[Organization] o ON o.OrgKey = p.OrgKey
WHERE h._date = CAST(GETDATE() AS DATE)
GROUP BY o.Name
ORDER BY TotalRows DESC

-- Summary line
SELECT 
    COUNT(DISTINCT o.Name)          AS TotalOrgs,
    COUNT(DISTINCT h.ProcessId)     AS TotalProcesses,
    COUNT(*)                        AS TotalRowsToday
FROM [Metrics].[MongoDBRightsizingAggregatedHourly] h
JOIN [MongoDB].[Process] p  ON p.ProcessId = h.ProcessId
JOIN [MongoDB].[Organization] o ON o.OrgKey = p.OrgKey
WHERE h._date = CAST(GETDATE() AS DATE)