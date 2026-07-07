-- Check if CpuAvgP95 and CpuMaxP95 have NULL values
-- in aggregated table for June
SELECT
    COUNT(*)                                AS TotalRows,
    SUM(CASE WHEN CpuAvgP95 IS NULL
             THEN 1 ELSE 0 END)            AS CpuAvgP95_Nulls,
    SUM(CASE WHEN CpuMaxP95 IS NULL
             THEN 1 ELSE 0 END)            AS CpuMaxP95_Nulls,
    SUM(CASE WHEN CpuAvgP95 IS NOT NULL
             THEN 1 ELSE 0 END)            AS CpuAvgP95_Valid,
    SUM(CASE WHEN CpuMaxP95 IS NOT NULL
             THEN 1 ELSE 0 END)            AS CpuMaxP95_Valid
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
GO

-- Also check which clusters have NULL P95 values
SELECT
    ClusterName,
    COUNT(*)                                AS TotalRows,
    SUM(CASE WHEN CpuAvgP95 IS NULL
             THEN 1 ELSE 0 END)            AS NullAvgP95,
    SUM(CASE WHEN CpuMaxP95 IS NULL
             THEN 1 ELSE 0 END)            AS NullMaxP95
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
GROUP BY ClusterName
HAVING SUM(CASE WHEN CpuAvgP95 IS NULL
                THEN 1 ELSE 0 END) > 0
OR     SUM(CASE WHEN CpuMaxP95 IS NULL
                THEN 1 ELSE 0 END) > 0
ORDER BY NullAvgP95 DESC
GO