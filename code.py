SELECT
    r.ClusterName,
    r.CurrentSku,
    s.ActualSku,
    CASE
        WHEN r.CurrentSku = s.ActualSku
        THEN 'Same ✅'
        ELSE 'Different ⚠️'
    END AS SkuMatch
FROM [Metrics].[MongoDBRightsizingRecommendations_STL] r
JOIN (
    SELECT
        Cluster AS ClusterName,
        MAX(Sku) AS ActualSku
    FROM [MongoDB].[Spend]
    WHERE FORMAT(CAST(UsageDate AS DATE),'yyyy-MM') = '2026-06'
    GROUP BY Cluster
) s ON s.ClusterName = r.ClusterName
WHERE r.Month = '2026-06'
AND   r.DayType = 'Weekday'
AND   r.HourType = 'BusinessHours'
ORDER BY SkuMatch DESC
GO