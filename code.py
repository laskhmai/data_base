SELECT DISTINCT
    a.InstanceSize,
    COUNT(*) AS HourCount
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
JOIN [MongoDB].[Clusters] c
    ON c.ClustersKey = a.ClusterKey
WHERE c.Name = 'labelh-dev'
AND FORMAT(a._date,'yyyy-MM') = '2026-06'
GROUP BY a.InstanceSize
ORDER BY HourCount DESC
GO