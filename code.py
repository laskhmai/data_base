-- Simple count, NO JOIN
SELECT COUNT(*)
FROM [MongoDB].[Clusters]
WHERE StateName IN ('IDLE','UPDATING')
AND   Paused = 0
-- Returns 327 ✅


SELECT
    c.Name,
    c.ClustersKey,
    c.StateName,
    c.CreateDate
FROM [MongoDB].[Clusters] c
WHERE c.StateName IN ('IDLE','UPDATING')
AND   c.Paused = 0
AND   c.ClustersKey NOT IN (
    SELECT DISTINCT ClusterKey
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
)
ORDER BY c.CreateDate DESC
GO