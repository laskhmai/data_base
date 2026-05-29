-- Find which clusters have missing MetaConfig
SELECT DISTINCT
    s.ClusterKey,
    s.ClusterName,
    s.InstanceSize,
    s.ProviderName,
    s.RegionName
FROM [Metrics].[MongoDBRightsizingAggregated5Min] s
WHERE s.MemResidentMaxPct = 0
AND   s.MemResidentMax    > 0  -- has memory data but no %
ORDER BY s.InstanceSize
GO

-- Is OpcInsert genuinely zero or missing data?
SELECT
    ClusterName,
    COUNT(*) AS TotalHours,
    SUM(CASE WHEN OpcInsertMax > 0 THEN 1 ELSE 0 END) AS HoursWithInserts,
    MAX(OpcInsertMax) AS PeakInserts
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GROUP BY ClusterName
HAVING MAX(OpcInsertMax) = 0  -- clusters with ZERO inserts ever
ORDER BY ClusterName
GO

C:\venvs\work_env\Scripts\pip install pyodbc