-- Find ALL duplicate Instance+Provider combos
-- that have multiple regions
SELECT
    Instance,
    Provider,
    COUNT(DISTINCT Region)  AS RegionCount,
    STRING_AGG(Region, ', ') AS Regions
FROM [Analytics].[MongoDBMetaConfig]
WHERE Tier = 'Standard'
GROUP BY Instance, Provider
HAVING COUNT(DISTINCT Region) > 1
ORDER BY Instance, Provider
GO