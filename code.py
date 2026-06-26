-- Is cmsonc-eob-prod-cluster
-- in our recommendations at all?
SELECT
    Month,
    ClusterName,
    DayType,
    HourType,
    Action,
    RecommendedSku,
    CurrentSku
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE ClusterName LIKE '%cmsonc-eob-prod%'
ORDER BY Month
GO