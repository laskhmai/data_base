-- =============================================
-- MongoDB Rightsizing 5Min Table Verification
-- Table: [Metrics].[MongoDBRightsizingAggregated5Min]
-- Run all steps after proc execution
-- =============================================


-- =============================================
-- Step 1: Basic Row Count
-- Expected: ~49,000 rows, ~280 clusters, 7 day range
-- =============================================
SELECT
    COUNT(*)                    AS TotalRows,
    COUNT(DISTINCT ClusterKey)  AS TotalClusters,
    MIN(_date)                  AS MinDate,
    MAX(_date)                  AS MaxDate
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GO


-- =============================================
-- Step 2: Duplicate Check
-- Expected: Zero rows returned
-- =============================================
SELECT
    ClusterKey,
    ClusterName,
    _date,
    _hour,
    COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GROUP BY
    ClusterKey,
    ClusterName,
    _date,
    _hour
HAVING COUNT(*) > 1
ORDER BY RowCount DESC
GO


-- =============================================
-- Step 3: Impossible Values Check
-- Expected: Zero rows returned
-- =============================================
SELECT
    ClusterName,
    InstanceSize,
    _date,
    _hour,
    CpuMax,
    MemResidentMaxPct,
    ConnUtilizationPct,
    CASE WHEN CpuMax             > 100 THEN 'CPU > 100%'          END AS CpuCheck,
    CASE WHEN MemResidentMaxPct  > 100 THEN 'Mem > 100%'          END AS MemCheck,
    CASE WHEN ConnUtilizationPct > 100 THEN 'Conn > 100%'         END AS ConnCheck,
    CASE WHEN CpuAvgP95          > CpuMax THEN 'P95 > Max'        END AS P95Check,
    CASE WHEN MemResidentAvg     > MemResidentMax THEN 'Avg > Max' END AS MemAvgCheck
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE CpuMax              > 100
OR    MemResidentMaxPct   > 100
OR    ConnUtilizationPct  > 100
OR    CpuAvgP95           > CpuMax
OR    MemResidentAvg      > MemResidentMax
ORDER BY ClusterName
GO


-- =============================================
-- Step 4: Column Population Check
-- Expected: All columns close to TotalRows
-- Any column at 0 means not collecting
-- =============================================
SELECT
    SUM(CASE WHEN CpuAvg             > 0 THEN 1 ELSE 0 END) AS CpuAvg_OK,
    SUM(CASE WHEN CpuAvgP95          > 0 THEN 1 ELSE 0 END) AS CpuAvgP95_OK,
    SUM(CASE WHEN CpuMax             > 0 THEN 1 ELSE 0 END) AS CpuMax_OK,
    SUM(CASE WHEN CpuMaxP95          > 0 THEN 1 ELSE 0 END) AS CpuMaxP95_OK,
    SUM(CASE WHEN MemResidentMax     > 0 THEN 1 ELSE 0 END) AS MemMax_OK,
    SUM(CASE WHEN MemResidentAvg     > 0 THEN 1 ELSE 0 END) AS MemAvg_OK,
    SUM(CASE WHEN MemAvailableMin    > 0 THEN 1 ELSE 0 END) AS MemAvail_OK,
    SUM(CASE WHEN MemResidentMaxPct  > 0 THEN 1 ELSE 0 END) AS MemMaxPct_OK,
    SUM(CASE WHEN MemResidentAvgPct  > 0 THEN 1 ELSE 0 END) AS MemAvgPct_OK,
    SUM(CASE WHEN MemResidentP95Pct  > 0 THEN 1 ELSE 0 END) AS MemP95Pct_OK,
    SUM(CASE WHEN NetInAvg           > 0 THEN 1 ELSE 0 END) AS NetInAvg_OK,
    SUM(CASE WHEN NetInMax           > 0 THEN 1 ELSE 0 END) AS NetInMax_OK,
    SUM(CASE WHEN NetOutAvg          > 0 THEN 1 ELSE 0 END) AS NetOutAvg_OK,
    SUM(CASE WHEN NetOutMax          > 0 THEN 1 ELSE 0 END) AS NetOutMax_OK,
    SUM(CASE WHEN NetRequestsMax     > 0 THEN 1 ELSE 0 END) AS NetReq_OK,
    SUM(CASE WHEN ConnectionsMax     > 0 THEN 1 ELSE 0 END) AS ConnMax_OK,
    SUM(CASE WHEN ConnUtilizationPct > 0 THEN 1 ELSE 0 END) AS ConnUtil_OK,
    SUM(CASE WHEN OpcQueryMax        > 0 THEN 1 ELSE 0 END) AS OpcQuery_OK,
    SUM(CASE WHEN OpcInsertMax       > 0 THEN 1 ELSE 0 END) AS OpcInsert_OK,
    COUNT(*)                                                 AS TotalRows
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GO


-- =============================================
-- Step 5: Spot Check cdr-uat (ClusterKey = 330)
-- Expected: 1 row per hour, ConnUtil < 10%, MemPct < 5%
-- =============================================
SELECT
    ClusterKey,
    ClusterName,
    _date,
    _hour,
    CpuMax,
    MemResidentMaxPct,
    ConnectionsMax,
    ConnUtilizationPct
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterKey = 330
ORDER BY _date, _hour
GO


-- =============================================
-- Step 6: Connection Fix Verification
-- Expected: Zero rows returned (no cluster over 100%)
-- =============================================
SELECT
    ClusterName,
    InstanceSize,
    MAX(ConnUtilizationPct) AS PeakConnPct,
    MAX(ConnectionsMax)     AS PeakConnections
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GROUP BY ClusterName, InstanceSize
HAVING MAX(ConnUtilizationPct) > 100
ORDER BY PeakConnPct DESC
GO


-- =============================================
-- Step 7: Math Validation — Percentages Match Raw Values
-- Expected: CalcMemPct = MemResidentMaxPct
--           CalcConnPct = ConnUtilizationPct
-- =============================================
SELECT
    ClusterName,
    InstanceSize,
    _date,
    _hour,
    MemResidentMax,
    MemResidentMaxPct,
    ConnectionsMax,
    ConnUtilizationPct
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterKey = 330
AND   _date      = (SELECT MAX(_date) FROM [Metrics].[MongoDBRightsizingAggregated5Min])
AND   _hour      = 8
GO


-- =============================================
-- Step 8: Rightsizing Preview
-- Expected: Each cluster shows ScaleUp / ScaleDown / Optimal
-- =============================================
SELECT
    ClusterName,
    InstanceSize,
    ROUND(MAX(CpuMax),  2)              AS PeakCpu,
    ROUND(AVG(CpuAvg),  2)              AS AvgCpu,
    SUM(CpuMaxGt50)                     AS HoursAbove50Pct,
    SUM(CpuMaxGt25)                     AS HoursAbove25Pct,
    ROUND(MAX(MemResidentMaxPct), 2)    AS PeakMemPct,
    ROUND(MAX(ConnUtilizationPct), 2)   AS PeakConnPct,
    CASE
        WHEN MAX(CpuMax)             > 50
          OR MAX(MemResidentMaxPct)  > 70
          OR MAX(ConnUtilizationPct) > 70
            THEN 'ScaleUp'
        WHEN AVG(CpuAvg)             < 25
         AND SUM(CpuMaxGt25)         = 0
         AND MAX(MemResidentMaxPct)  < 30
            THEN 'ScaleDown'
        ELSE 'Optimal'
    END                                 AS Recommendation
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GROUP BY ClusterName, InstanceSize
ORDER BY Recommendation, ClusterName
GO