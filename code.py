-- Do these 13 clusters have Weekend data in May?
SELECT
    a.ClusterKey,
    a.ClusterName,
    a.[type],
    COUNT(*)        AS Rows,
    MIN(a._date)    AS From,
    MAX(a._date)    AS To
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
WHERE FORMAT(a._date,'yyyy-MM') = '2026-05'
AND   a.ClusterKey IN (
    SELECT ClusterKey
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    WHERE Month    = '2026-05'
    GROUP BY ClusterKey
    HAVING COUNT(*) < 3
)
GROUP BY a.ClusterKey, a.ClusterName, a.[type]
ORDER BY a.ClusterName, a.[type]
GO