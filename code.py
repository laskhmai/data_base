-- Check when STL table rows were inserted
SELECT
    MIN(AuditUtc) AS FirstInserted,
    MAX(AuditUtc) AS LastInserted,
    COUNT(*)      AS TotalRows
FROM [Metrics].[MongoDBRightsizingRecommendations_STL]
WHERE Month = '2026-06'
GO