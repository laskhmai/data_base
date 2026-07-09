-- Check exact stored value in MetaConfig
SELECT
    Instance,
    CostPrHour,
    CAST(CostPrHour AS DECIMAL(10,4)) AS CostExact
FROM [Analytics].[MongoDBMetaConfig]
WHERE Provider = 'GCP'
AND   Region   = 'CENTRAL_US'
AND   Instance = 'M10'
GO

-- Check what recommendations table stored
SELECT
    ClusterName,
    CurrentSku,
    CurrentCostPrHour,
    CAST(CurrentCostPrHour AS DECIMAL(10,4)) AS CostExact
FROM [Metrics].[MongoDBRightsizingRecommendations_STL]
WHERE ClusterName = 'CAP-INT-Cluster-01'
AND   Month = '2026-06'
GO