Hi team, same approach as last time but a DIFFERENT target. This time clear soft_delete_enabled off the key vault resource, not the credential-key-vault data source. One-line steps (run on INT first, then Prod):

1. Find address: terraform state list | grep azurerm_key_vault   (pick the one WITHOUT data. prefix)
SELECT COUNT(*) AS BadRows
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Action = 'Downsize'
AND Comment LIKE '%Intensive%'

SELECT
    ClusterName,
    CurrentSku,
    RecommendedSku,
    Action,
    DayType,
    HourType,
    Comment
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE ClusterName IN (
    'cdr-uat',
    'cwih-cp-mgmt-prod',
    'consumer-interops-uat'
)
AND DayType  = 'Weekday'
AND HourType = 'BusinessHours'
ORDER BY ClusterName

SELECT DISTINCT
    a.ClusterKey,
    a.ClusterName
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
WHERE NOT EXISTS (
    SELECT 1
    FROM [Metrics].[MongoDBRightsizingRecommendations] r
    WHERE r.ClusterKey = a.ClusterKey
)