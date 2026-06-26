-- Summary of CpuMax for this cluster
SELECT
    ClusterName,
    FORMAT(_date,'yyyy-MM')     AS Month,
    ROUND(AVG(CpuMax),  2)     AS AvgOfCpuMax,
    ROUND(MAX(CpuMax),  2)     AS MaxCpuMax,
    ROUND(AVG(CpuAvg),  2)     AS AvgCpuAvg,
    ROUND(MAX(CpuAvgP95),2)    AS MaxCpuAvgP95,
    ROUND(MAX(CpuMaxP95),2)    AS MaxCpuMaxP95,
    COUNT(*)                    AS TotalRows
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName LIKE '%cmsonc-eob-prod%'
GROUP BY
    ClusterName,
    FORMAT(_date,'yyyy-MM')
ORDER BY Month
GO