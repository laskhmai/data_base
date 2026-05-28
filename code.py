-- PREVIEW — see all duplicates first
WITH CTE AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY SkuName, Provider, Region
            ORDER BY Id
        ) AS rn
    FROM [Analytics].[MongoDBMetaConfig]
)
SELECT * FROM CTE WHERE rn > 1
ORDER BY SkuName, Provider, Region
GO