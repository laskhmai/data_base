-- =============================================
-- MongoDB Rightsizing — Simulated Metrics + Efficiency
-- Schema  : Metrics
-- Project : MongoDB Rightsizing — COSD Team Humana
-- DevOps  : 9009227
-- =============================================
-- CONTAINS:
--   Part 1 → CREATE TABLE MongoDBRightsizingSimulatedMetrics
--   Part 2 → usp_MongoDBRightsizingSimulatedMetrics
--   Part 3 → usp_MongoDBRightsizingEfficiency
-- =============================================

-- =============================================
-- PART 1 — CREATE SimulatedMetrics TABLE
-- =============================================

DROP TABLE IF EXISTS [Metrics].[MongoDBRightsizingSimulatedMetrics]
GO

CREATE TABLE [Metrics].[MongoDBRightsizingSimulatedMetrics]
(
    -- Identity
    ClusterKey          INT            NULL,
    ClusterName         NVARCHAR(255)  NULL,
    CurrentSku          NVARCHAR(50)   NULL,
    [Date]              DATE           NULL,
    [Hour]              INT            NULL,
    DayType             NVARCHAR(20)   NULL,
    HourType            NVARCHAR(30)   NULL,

    -- Raw current metrics
    CpuAvg              FLOAT          NULL,
    CpuMax              FLOAT          NULL,
    CpuAvgP95           FLOAT          NULL,
    CpuMaxP95           FLOAT          NULL,
    MemAvg              FLOAT          NULL,
    MemMax              FLOAT          NULL,
    MemAvgP95           FLOAT          NULL,
    MemMaxP95           FLOAT          NULL,
    ConnAvg             FLOAT          NULL,
    ConnMax             FLOAT          NULL,

    -- Recommendation SKUs
    RecommendedSku      NVARCHAR(100)  NULL,   -- e.g. "M50, M50-low-CPU"
    LowCpuSku           NVARCHAR(50)   NULL,   -- e.g. "M50-low-CPU"

    -- Within family projections (Standard SKU)
    nCpuAvgWithin       FLOAT          NULL,
    nCpuMaxWithin       FLOAT          NULL,
    nCpuAvgP95Within    FLOAT          NULL,
    nCpuMaxP95Within    FLOAT          NULL,
    nMemAvgWithin       FLOAT          NULL,
    nMemMaxWithin       FLOAT          NULL,
    nMemAvgP95Within    FLOAT          NULL,
    nMemMaxP95Within    FLOAT          NULL,
    nConnAvgWithin      FLOAT          NULL,
    nConnMaxWithin      FLOAT          NULL,

    -- Low-CPU projections
    nCpuAvgLowCpu       FLOAT          NULL,
    nCpuMaxLowCpu       FLOAT          NULL,
    nCpuAvgP95LowCpu    FLOAT          NULL,
    nCpuMaxP95LowCpu    FLOAT          NULL,
    nMemAvgLowCpu       FLOAT          NULL,
    nMemMaxLowCpu       FLOAT          NULL,
    nMemAvgP95LowCpu    FLOAT          NULL,
    nMemMaxP95LowCpu    FLOAT          NULL,
    nConnAvgLowCpu      FLOAT          NULL,
    nConnMaxLowCpu      FLOAT          NULL
)
WITH (HEAP)
GO


-- =============================================
-- PART 2 — usp_MongoDBRightsizingSimulatedMetrics
-- Reads aggregated table + recommendations
-- Calculates projected metrics for Standard + Low-CPU SKUs
-- UPSERTS into MongoDBRightsizingSimulatedMetrics
-- Pattern: Postgres usp_PostgreSQLRightsizingSimulatedMetrics
-- =============================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROC [Metrics].[usp_MongoDBRightsizingSimulatedMetrics]
    @LastMonth CHAR(7)
AS
BEGIN
    SET NOCOUNT ON;

    -- =========================================
    -- Drop temp tables
    -- =========================================
    IF OBJECT_ID('tempdb..#SkuConfig')       IS NOT NULL DROP TABLE #SkuConfig;
    IF OBJECT_ID('tempdb..#Recommendations') IS NOT NULL DROP TABLE #Recommendations;
    IF OBJECT_ID('tempdb..#Simulated')       IS NOT NULL DROP TABLE #Simulated;

    -- =========================================
    -- Load MetaConfig (vCores, RAM_GB, ConnectionLimit per SKU)
    -- =========================================
    SELECT
        Instance            AS Sku,
        CAST(vCores         AS FLOAT) AS vCores,
        CAST(MemorySizeGB   AS FLOAT) AS RAM_GB,
        CAST(ConnectionLimit AS FLOAT) AS ConnectionLimit
    INTO #SkuConfig
    FROM [Analytics].[MongoDBMetaConfig]
    WHERE Tier NOT IN ('Free', 'Flex');

    -- =========================================
    -- Load recommendations for this month
    -- =========================================
    SELECT *
    INTO #Recommendations
    FROM [Metrics].[MongoDBRightsizingRecommendations]
    WHERE Month = @LastMonth;

    -- =========================================
    -- Calculate simulated metrics
    -- Formula (Postgres pattern):
    --   nCpuWithin = CpuAvg × (currentVCores / withinVCores)
    --   nMemWithin = MemAvg × (currentRAM   / withinRAM)
    --   nConnWithin= ConnAvg× (currentConn  / withinConn)
    -- Same for Low-CPU alternative
    -- =========================================
    SELECT
        a.ClusterKey,
        a.ClusterName,
        a.InstanceSize                                      AS CurrentSku,
        a._date                                             AS [Date],
        a._hour                                             AS [Hour],
        a.[type]                                            AS DayType,
        CASE WHEN a.[type] = 'Weekend'
             THEN 'Weekend'
             ELSE a.businessHour END                        AS HourType,

        -- Raw current metrics
        a.CpuAvg,
        a.CpuMax,
        a.CpuAvgP95,
        a.CpuMaxP95,
        a.MemResidentAvgPct                                 AS MemAvg,
        a.MemResidentMaxPct                                 AS MemMax,
        a.MemResidentP95Pct                                 AS MemAvgP95,
        a.MemResidentP95Pct                                 AS MemMaxP95,
        a.ConnUtilizationPct                                AS ConnAvg,
        a.ConnUtilizationPct                                AS ConnMax,

        -- Recommendation SKUs
        r.RecommendedSku,
        r.LowCpuSku,

        -- Within family (Standard) projections
        CASE WHEN wf.vCores          > 0
             THEN a.CpuAvg           * cs.vCores          / wf.vCores          ELSE NULL END AS nCpuAvgWithin,
        CASE WHEN wf.vCores          > 0
             THEN a.CpuMax           * cs.vCores          / wf.vCores          ELSE NULL END AS nCpuMaxWithin,
        CASE WHEN wf.vCores          > 0
             THEN a.CpuAvgP95        * cs.vCores          / wf.vCores          ELSE NULL END AS nCpuAvgP95Within,
        CASE WHEN wf.vCores          > 0
             THEN a.CpuMaxP95        * cs.vCores          / wf.vCores          ELSE NULL END AS nCpuMaxP95Within,
        CASE WHEN wf.RAM_GB          > 0
             THEN a.MemResidentAvgPct* cs.RAM_GB          / wf.RAM_GB          ELSE NULL END AS nMemAvgWithin,
        CASE WHEN wf.RAM_GB          > 0
             THEN a.MemResidentMaxPct* cs.RAM_GB          / wf.RAM_GB          ELSE NULL END AS nMemMaxWithin,
        CASE WHEN wf.RAM_GB          > 0
             THEN a.MemResidentP95Pct* cs.RAM_GB          / wf.RAM_GB          ELSE NULL END AS nMemAvgP95Within,
        CASE WHEN wf.RAM_GB          > 0
             THEN a.MemResidentP95Pct* cs.RAM_GB          / wf.RAM_GB          ELSE NULL END AS nMemMaxP95Within,
        CASE WHEN wf.ConnectionLimit > 0
             THEN a.ConnUtilizationPct * cs.ConnectionLimit / wf.ConnectionLimit ELSE NULL END AS nConnAvgWithin,
        CASE WHEN wf.ConnectionLimit > 0
             THEN a.ConnUtilizationPct * cs.ConnectionLimit / wf.ConnectionLimit ELSE NULL END AS nConnMaxWithin,

        -- Low-CPU projections
        CASE WHEN lc.vCores          > 0
             THEN a.CpuAvg           * cs.vCores          / lc.vCores          ELSE NULL END AS nCpuAvgLowCpu,
        CASE WHEN lc.vCores          > 0
             THEN a.CpuMax           * cs.vCores          / lc.vCores          ELSE NULL END AS nCpuMaxLowCpu,
        CASE WHEN lc.vCores          > 0
             THEN a.CpuAvgP95        * cs.vCores          / lc.vCores          ELSE NULL END AS nCpuAvgP95LowCpu,
        CASE WHEN lc.vCores          > 0
             THEN a.CpuMaxP95        * cs.vCores          / lc.vCores          ELSE NULL END AS nCpuMaxP95LowCpu,
        CASE WHEN lc.RAM_GB          > 0
             THEN a.MemResidentAvgPct* cs.RAM_GB          / lc.RAM_GB          ELSE NULL END AS nMemAvgLowCpu,
        CASE WHEN lc.RAM_GB          > 0
             THEN a.MemResidentMaxPct* cs.RAM_GB          / lc.RAM_GB          ELSE NULL END AS nMemMaxLowCpu,
        CASE WHEN lc.RAM_GB          > 0
             THEN a.MemResidentP95Pct* cs.RAM_GB          / lc.RAM_GB          ELSE NULL END AS nMemAvgP95LowCpu,
        CASE WHEN lc.RAM_GB          > 0
             THEN a.MemResidentP95Pct* cs.RAM_GB          / lc.RAM_GB          ELSE NULL END AS nMemMaxP95LowCpu,
        CASE WHEN lc.ConnectionLimit > 0
             THEN a.ConnUtilizationPct * cs.ConnectionLimit / lc.ConnectionLimit ELSE NULL END AS nConnAvgLowCpu,
        CASE WHEN lc.ConnectionLimit > 0
             THEN a.ConnUtilizationPct * cs.ConnectionLimit / lc.ConnectionLimit ELSE NULL END AS nConnMaxLowCpu

    INTO #Simulated
    FROM  [Metrics].[MongoDBRightsizingAggregated5Min] a
    INNER JOIN #Recommendations r
        ON  r.ClusterKey = a.ClusterKey
        AND r.DayType    = a.[type]
        AND (   a.[type]       = 'Weekend'
             OR r.HourType     = a.businessHour)
    INNER JOIN #SkuConfig cs
        ON  cs.Sku = a.InstanceSize
    LEFT  JOIN #SkuConfig wf
        ON  LEFT(r.RecommendedSku,
                 CHARINDEX(',', r.RecommendedSku + ',') - 1) = wf.Sku
    LEFT  JOIN #SkuConfig lc
        ON  r.LowCpuSku = lc.Sku
    WHERE FORMAT(a._date, 'yyyy-MM') = @LastMonth;

    -- =========================================
    -- UPSERT — UPDATE existing rows
    -- =========================================
    UPDATE T
    SET
        T.CpuAvg           = S.CpuAvg,
        T.CpuMax           = S.CpuMax,
        T.CpuAvgP95        = S.CpuAvgP95,
        T.CpuMaxP95        = S.CpuMaxP95,
        T.MemAvg           = S.MemAvg,
        T.MemMax           = S.MemMax,
        T.MemAvgP95        = S.MemAvgP95,
        T.MemMaxP95        = S.MemMaxP95,
        T.ConnAvg          = S.ConnAvg,
        T.ConnMax          = S.ConnMax,
        T.RecommendedSku   = S.RecommendedSku,
        T.LowCpuSku        = S.LowCpuSku,
        T.nCpuAvgWithin    = S.nCpuAvgWithin,
        T.nCpuMaxWithin    = S.nCpuMaxWithin,
        T.nCpuAvgP95Within = S.nCpuAvgP95Within,
        T.nCpuMaxP95Within = S.nCpuMaxP95Within,
        T.nMemAvgWithin    = S.nMemAvgWithin,
        T.nMemMaxWithin    = S.nMemMaxWithin,
        T.nMemAvgP95Within = S.nMemAvgP95Within,
        T.nMemMaxP95Within = S.nMemMaxP95Within,
        T.nConnAvgWithin   = S.nConnAvgWithin,
        T.nConnMaxWithin   = S.nConnMaxWithin,
        T.nCpuAvgLowCpu    = S.nCpuAvgLowCpu,
        T.nCpuMaxLowCpu    = S.nCpuMaxLowCpu,
        T.nCpuAvgP95LowCpu = S.nCpuAvgP95LowCpu,
        T.nCpuMaxP95LowCpu = S.nCpuMaxP95LowCpu,
        T.nMemAvgLowCpu    = S.nMemAvgLowCpu,
        T.nMemMaxLowCpu    = S.nMemMaxLowCpu,
        T.nMemAvgP95LowCpu = S.nMemAvgP95LowCpu,
        T.nMemMaxP95LowCpu = S.nMemMaxP95LowCpu,
        T.nConnAvgLowCpu   = S.nConnAvgLowCpu,
        T.nConnMaxLowCpu   = S.nConnMaxLowCpu
    FROM [Metrics].[MongoDBRightsizingSimulatedMetrics] T
    JOIN #Simulated S
        ON  T.ClusterKey = S.ClusterKey
        AND T.[Date]     = S.[Date]
        AND T.[Hour]     = S.[Hour]
        AND T.DayType    = S.DayType
        AND T.HourType   = S.HourType
        AND T.CurrentSku = S.CurrentSku;

    -- =========================================
    -- UPSERT — INSERT new rows
    -- =========================================
    INSERT INTO [Metrics].[MongoDBRightsizingSimulatedMetrics]
    (
        ClusterKey, ClusterName, CurrentSku,
        [Date], [Hour], DayType, HourType,
        CpuAvg, CpuMax, CpuAvgP95, CpuMaxP95,
        MemAvg, MemMax, MemAvgP95, MemMaxP95,
        ConnAvg, ConnMax,
        RecommendedSku, LowCpuSku,
        nCpuAvgWithin,    nCpuMaxWithin,    nCpuAvgP95Within, nCpuMaxP95Within,
        nMemAvgWithin,    nMemMaxWithin,     nMemAvgP95Within, nMemMaxP95Within,
        nConnAvgWithin,   nConnMaxWithin,
        nCpuAvgLowCpu,    nCpuMaxLowCpu,    nCpuAvgP95LowCpu, nCpuMaxP95LowCpu,
        nMemAvgLowCpu,    nMemMaxLowCpu,     nMemAvgP95LowCpu, nMemMaxP95LowCpu,
        nConnAvgLowCpu,   nConnMaxLowCpu
    )
    SELECT
        S.ClusterKey, S.ClusterName, S.CurrentSku,
        S.[Date], S.[Hour], S.DayType, S.HourType,
        S.CpuAvg, S.CpuMax, S.CpuAvgP95, S.CpuMaxP95,
        S.MemAvg, S.MemMax, S.MemAvgP95, S.MemMaxP95,
        S.ConnAvg, S.ConnMax,
        S.RecommendedSku, S.LowCpuSku,
        S.nCpuAvgWithin,    S.nCpuMaxWithin,    S.nCpuAvgP95Within, S.nCpuMaxP95Within,
        S.nMemAvgWithin,    S.nMemMaxWithin,     S.nMemAvgP95Within, S.nMemMaxP95Within,
        S.nConnAvgWithin,   S.nConnMaxWithin,
        S.nCpuAvgLowCpu,    S.nCpuMaxLowCpu,    S.nCpuAvgP95LowCpu, S.nCpuMaxP95LowCpu,
        S.nMemAvgLowCpu,    S.nMemMaxLowCpu,     S.nMemAvgP95LowCpu, S.nMemMaxP95LowCpu,
        S.nConnAvgLowCpu,   S.nConnMaxLowCpu
    FROM #Simulated S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [Metrics].[MongoDBRightsizingSimulatedMetrics] T
        WHERE T.ClusterKey = S.ClusterKey
        AND   T.[Date]     = S.[Date]
        AND   T.[Hour]     = S.[Hour]
        AND   T.DayType    = S.DayType
        AND   T.HourType   = S.HourType
        AND   T.CurrentSku = S.CurrentSku
    );

END
GO


-- =============================================
-- PART 3 — usp_MongoDBRightsizingEfficiency
-- Reads SimulatedMetrics table
-- Calculates sigmoid-based efficiency scores
-- Aggregates AVG + P95 per cluster/daytype/hourtype
-- Creates JSON for CurrentEfficiency, WithinEfficiency, LowCpuEfficiency
-- UPDATES MongoDBRightsizingRecommendations table
-- Pattern: Postgres usp_PostgreSQLRightsizingEfficiency
-- NOTE: PERCENTILE_CONT OVER not supported in Synapse
--       Using ROW_NUMBER + CEILING(N x 0.95) instead
-- =============================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROC [Metrics].[usp_MongoDBRightsizingEfficiency]
    @LastMonth CHAR(7)
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('tempdb..#EffResult') IS NOT NULL DROP TABLE #EffResult;

    -- =========================================
    -- STEP 1 — Calculate efficiency per row
    -- Sigmoid formula (Postgres pattern):
    --   Efficiency = 0.02 + 0.98 × sigmoid(avg - 50) × stability
    --   sigmoid(x) = 1 / (1 + EXP(-0.09 × x))
    --   stability  = 0.5 + 0.5
    --                × EXP(-0.05 × CASE WHEN max < 25 THEN 25 - max ELSE 0 END)
    --                × (1 - MIN(ABS(avg - avgP95) / NULLIF(avgP95,0), 1))
    --                × (1 - MIN(ABS(max - maxP95) / NULLIF(maxP95,0), 1))
    -- =========================================
    ;WITH EffBase AS (
        SELECT
            FORMAT(CAST([Date] AS DATE), 'yyyy-MM') AS MonthName,
            ClusterKey,
            DayType,
            HourType,
            CurrentSku,

            -- Current CPU Efficiency
            0.02 + (1.0 - 0.02) * (
                1.0 / (1.0 + EXP(-0.09 * (CpuAvg - 50)))
            ) * (
                0.5 + 0.5
                * EXP(-0.05 * CASE WHEN CpuMax < 25 THEN 25 - CpuMax ELSE 0 END)
                * (1 - CASE WHEN NULLIF(CpuAvgP95,0) IS NULL THEN 0
                            WHEN ABS(CpuAvg - CpuAvgP95) / CpuAvgP95 > 1 THEN 1
                            ELSE ABS(CpuAvg - CpuAvgP95) / CpuAvgP95 END)
                * (1 - CASE WHEN NULLIF(CpuMaxP95,0) IS NULL THEN 0
                            WHEN ABS(CpuMax - CpuMaxP95) / CpuMaxP95 > 1 THEN 1
                            ELSE ABS(CpuMax - CpuMaxP95) / CpuMaxP95 END)
            )                                           AS CpuEffCurrent,

            -- Current Memory Efficiency
            0.02 + (1.0 - 0.02) * (
                1.0 / (1.0 + EXP(-0.09 * (MemAvg - 50)))
            ) * (
                0.5 + 0.5
                * EXP(-0.05 * CASE WHEN MemMax < 25 THEN 25 - MemMax ELSE 0 END)
                * (1 - CASE WHEN NULLIF(MemAvgP95,0) IS NULL THEN 0
                            WHEN ABS(MemAvg - MemAvgP95) / MemAvgP95 > 1 THEN 1
                            ELSE ABS(MemAvg - MemAvgP95) / MemAvgP95 END)
                * (1 - CASE WHEN NULLIF(MemMaxP95,0) IS NULL THEN 0
                            WHEN ABS(MemMax - MemMaxP95) / MemMaxP95 > 1 THEN 1
                            ELSE ABS(MemMax - MemMaxP95) / MemMaxP95 END)
            )                                           AS MemEffCurrent,

            -- Within Family CPU Efficiency
            0.02 + (1.0 - 0.02) * (
                1.0 / (1.0 + EXP(-0.09 * (COALESCE(nCpuAvgWithin,0) - 50)))
            ) * (
                0.5 + 0.5
                * EXP(-0.05 * CASE WHEN COALESCE(nCpuMaxWithin,0) < 25 THEN 25 - COALESCE(nCpuMaxWithin,0) ELSE 0 END)
                * (1 - CASE WHEN NULLIF(nCpuAvgP95Within,0) IS NULL THEN 0
                            WHEN ABS(COALESCE(nCpuAvgWithin,0) - COALESCE(nCpuAvgP95Within,0)) / nCpuAvgP95Within > 1 THEN 1
                            ELSE ABS(COALESCE(nCpuAvgWithin,0) - COALESCE(nCpuAvgP95Within,0)) / nCpuAvgP95Within END)
                * (1 - CASE WHEN NULLIF(nCpuMaxP95Within,0) IS NULL THEN 0
                            WHEN ABS(COALESCE(nCpuMaxWithin,0) - COALESCE(nCpuMaxP95Within,0)) / nCpuMaxP95Within > 1 THEN 1
                            ELSE ABS(COALESCE(nCpuMaxWithin,0) - COALESCE(nCpuMaxP95Within,0)) / nCpuMaxP95Within END)
            )                                           AS CpuEffWithin,

            -- Within Family Memory Efficiency
            0.02 + (1.0 - 0.02) * (
                1.0 / (1.0 + EXP(-0.09 * (COALESCE(nMemAvgWithin,0) - 50)))
            ) * (
                0.5 + 0.5
                * EXP(-0.05 * CASE WHEN COALESCE(nMemMaxWithin,0) < 25 THEN 25 - COALESCE(nMemMaxWithin,0) ELSE 0 END)
                * (1 - CASE WHEN NULLIF(nMemAvgP95Within,0) IS NULL THEN 0
                            WHEN ABS(COALESCE(nMemAvgWithin,0) - COALESCE(nMemAvgP95Within,0)) / nMemAvgP95Within > 1 THEN 1
                            ELSE ABS(COALESCE(nMemAvgWithin,0) - COALESCE(nMemAvgP95Within,0)) / nMemAvgP95Within END)
                * (1 - CASE WHEN NULLIF(nMemMaxP95Within,0) IS NULL THEN 0
                            WHEN ABS(COALESCE(nMemMaxWithin,0) - COALESCE(nMemMaxP95Within,0)) / nMemMaxP95Within > 1 THEN 1
                            ELSE ABS(COALESCE(nMemMaxWithin,0) - COALESCE(nMemMaxP95Within,0)) / nMemMaxP95Within END)
            )                                           AS MemEffWithin,

            -- Low-CPU CPU Efficiency
            0.02 + (1.0 - 0.02) * (
                1.0 / (1.0 + EXP(-0.09 * (COALESCE(nCpuAvgLowCpu,0) - 50)))
            ) * (
                0.5 + 0.5
                * EXP(-0.05 * CASE WHEN COALESCE(nCpuMaxLowCpu,0) < 25 THEN 25 - COALESCE(nCpuMaxLowCpu,0) ELSE 0 END)
                * (1 - CASE WHEN NULLIF(nCpuAvgP95LowCpu,0) IS NULL THEN 0
                            WHEN ABS(COALESCE(nCpuAvgLowCpu,0) - COALESCE(nCpuAvgP95LowCpu,0)) / nCpuAvgP95LowCpu > 1 THEN 1
                            ELSE ABS(COALESCE(nCpuAvgLowCpu,0) - COALESCE(nCpuAvgP95LowCpu,0)) / nCpuAvgP95LowCpu END)
                * (1 - CASE WHEN NULLIF(nCpuMaxP95LowCpu,0) IS NULL THEN 0
                            WHEN ABS(COALESCE(nCpuMaxLowCpu,0) - COALESCE(nCpuMaxP95LowCpu,0)) / nCpuMaxP95LowCpu > 1 THEN 1
                            ELSE ABS(COALESCE(nCpuMaxLowCpu,0) - COALESCE(nCpuMaxP95LowCpu,0)) / nCpuMaxP95LowCpu END)
            )                                           AS CpuEffLowCpu,

            -- Low-CPU Memory Efficiency
            0.02 + (1.0 - 0.02) * (
                1.0 / (1.0 + EXP(-0.09 * (COALESCE(nMemAvgLowCpu,0) - 50)))
            ) * (
                0.5 + 0.5
                * EXP(-0.05 * CASE WHEN COALESCE(nMemMaxLowCpu,0) < 25 THEN 25 - COALESCE(nMemMaxLowCpu,0) ELSE 0 END)
                * (1 - CASE WHEN NULLIF(nMemAvgP95LowCpu,0) IS NULL THEN 0
                            WHEN ABS(COALESCE(nMemAvgLowCpu,0) - COALESCE(nMemAvgP95LowCpu,0)) / nMemAvgP95LowCpu > 1 THEN 1
                            ELSE ABS(COALESCE(nMemAvgLowCpu,0) - COALESCE(nMemAvgP95LowCpu,0)) / nMemAvgP95LowCpu END)
                * (1 - CASE WHEN NULLIF(nMemMaxP95LowCpu,0) IS NULL THEN 0
                            WHEN ABS(COALESCE(nMemMaxLowCpu,0) - COALESCE(nMemMaxP95LowCpu,0)) / nMemMaxP95LowCpu > 1 THEN 1
                            ELSE ABS(COALESCE(nMemMaxLowCpu,0) - COALESCE(nMemMaxP95LowCpu,0)) / nMemMaxP95LowCpu END)
            )                                           AS MemEffLowCpu

        FROM [Metrics].[MongoDBRightsizingSimulatedMetrics]
        WHERE FORMAT(CAST([Date] AS DATE), 'yyyy-MM') = @LastMonth
    ),

    -- =========================================
    -- STEP 2 — Aggregate AVG per cluster/slice
    -- =========================================
    EffAgg AS (
        SELECT DISTINCT
            MonthName,
            ClusterKey,
            DayType,
            HourType,
            CurrentSku,

            -- AVG efficiency scores
            AVG(CpuEffCurrent) OVER (PARTITION BY MonthName, ClusterKey, DayType, HourType, CurrentSku) AS AvgCpuCurrent,
            AVG(MemEffCurrent) OVER (PARTITION BY MonthName, ClusterKey, DayType, HourType, CurrentSku) AS AvgMemCurrent,
            AVG(CpuEffWithin)  OVER (PARTITION BY MonthName, ClusterKey, DayType, HourType, CurrentSku) AS AvgCpuWithin,
            AVG(MemEffWithin)  OVER (PARTITION BY MonthName, ClusterKey, DayType, HourType, CurrentSku) AS AvgMemWithin,
            AVG(CpuEffLowCpu)  OVER (PARTITION BY MonthName, ClusterKey, DayType, HourType, CurrentSku) AS AvgCpuLowCpu,
            AVG(MemEffLowCpu)  OVER (PARTITION BY MonthName, ClusterKey, DayType, HourType, CurrentSku) AS AvgMemLowCpu,

            -- ROW_NUMBER for P95 calculation (Synapse compatible — no PERCENTILE_CONT OVER)
            ROW_NUMBER() OVER (PARTITION BY MonthName, ClusterKey, DayType, HourType, CurrentSku ORDER BY CpuEffCurrent ASC) AS RnCpuCurrent,
            ROW_NUMBER() OVER (PARTITION BY MonthName, ClusterKey, DayType, HourType, CurrentSku ORDER BY MemEffCurrent ASC) AS RnMemCurrent,
            ROW_NUMBER() OVER (PARTITION BY MonthName, ClusterKey, DayType, HourType, CurrentSku ORDER BY CpuEffWithin  ASC) AS RnCpuWithin,
            ROW_NUMBER() OVER (PARTITION BY MonthName, ClusterKey, DayType, HourType, CurrentSku ORDER BY MemEffWithin  ASC) AS RnMemWithin,
            ROW_NUMBER() OVER (PARTITION BY MonthName, ClusterKey, DayType, HourType, CurrentSku ORDER BY CpuEffLowCpu  ASC) AS RnCpuLowCpu,
            ROW_NUMBER() OVER (PARTITION BY MonthName, ClusterKey, DayType, HourType, CurrentSku ORDER BY MemEffLowCpu  ASC) AS RnMemLowCpu,
            COUNT(*)     OVER (PARTITION BY MonthName, ClusterKey, DayType, HourType, CurrentSku)                             AS Cnt,

            -- Raw values for P95 selection
            CpuEffCurrent, MemEffCurrent,
            CpuEffWithin,  MemEffWithin,
            CpuEffLowCpu,  MemEffLowCpu
        FROM EffBase
    ),

    -- =========================================
    -- STEP 3 — Select P95 values per cluster/slice
    -- =========================================
    EffP95 AS (
        SELECT
            MonthName, ClusterKey, DayType, HourType, CurrentSku,
            MAX(AvgCpuCurrent) AS AvgCpuCurrent,
            MAX(AvgMemCurrent) AS AvgMemCurrent,
            MAX(AvgCpuWithin)  AS AvgCpuWithin,
            MAX(AvgMemWithin)  AS AvgMemWithin,
            MAX(AvgCpuLowCpu)  AS AvgCpuLowCpu,
            MAX(AvgMemLowCpu)  AS AvgMemLowCpu,
            MAX(CASE WHEN RnCpuCurrent = CAST(CEILING(Cnt * 0.95) AS INT) THEN CpuEffCurrent END) AS P95CpuCurrent,
            MAX(CASE WHEN RnMemCurrent = CAST(CEILING(Cnt * 0.95) AS INT) THEN MemEffCurrent END) AS P95MemCurrent,
            MAX(CASE WHEN RnCpuWithin  = CAST(CEILING(Cnt * 0.95) AS INT) THEN CpuEffWithin  END) AS P95CpuWithin,
            MAX(CASE WHEN RnMemWithin  = CAST(CEILING(Cnt * 0.95) AS INT) THEN MemEffWithin  END) AS P95MemWithin,
            MAX(CASE WHEN RnCpuLowCpu  = CAST(CEILING(Cnt * 0.95) AS INT) THEN CpuEffLowCpu  END) AS P95CpuLowCpu,
            MAX(CASE WHEN RnMemLowCpu  = CAST(CEILING(Cnt * 0.95) AS INT) THEN MemEffLowCpu  END) AS P95MemLowCpu
        FROM EffAgg
        GROUP BY MonthName, ClusterKey, DayType, HourType, CurrentSku
    )

    -- =========================================
    -- STEP 4 — Build JSON + INSERT into temp
    -- =========================================
    SELECT
        MonthName,
        ClusterKey,
        DayType,
        HourType,
        CurrentSku,

        -- JSON: CurrentEfficiency
        '{ "CpuEfficiency": "'     + FORMAT(AvgCpuCurrent * 100, 'N2') +
        '", "CpuEfficiencyP95": "' + FORMAT(P95CpuCurrent * 100, 'N2') +
        '", "MemEfficiency": "'    + FORMAT(AvgMemCurrent * 100, 'N2') +
        '", "MemEfficiencyP95": "' + FORMAT(P95MemCurrent * 100, 'N2') + '" }'
            AS CurrentEfficiency,

        -- JSON: WithinEfficiency (Standard SKU projections)
        '{ "CpuEfficiency": "'     + FORMAT(AvgCpuWithin  * 100, 'N2') +
        '", "CpuEfficiencyP95": "' + FORMAT(P95CpuWithin  * 100, 'N2') +
        '", "MemEfficiency": "'    + FORMAT(AvgMemWithin  * 100, 'N2') +
        '", "MemEfficiencyP95": "' + FORMAT(P95MemWithin  * 100, 'N2') + '" }'
            AS WithinEfficiency,

        -- JSON: LowCpuEfficiency (Low-CPU SKU projections)
        CASE WHEN AvgCpuLowCpu IS NOT NULL
        THEN
            '{ "CpuEfficiency": "'     + FORMAT(AvgCpuLowCpu  * 100, 'N2') +
            '", "CpuEfficiencyP95": "' + FORMAT(P95CpuLowCpu  * 100, 'N2') +
            '", "MemEfficiency": "'    + FORMAT(AvgMemLowCpu  * 100, 'N2') +
            '", "MemEfficiencyP95": "' + FORMAT(P95MemLowCpu  * 100, 'N2') + '" }'
        ELSE NULL END
            AS LowCpuEfficiency

    INTO #EffResult
    FROM EffP95;

    -- =========================================
    -- STEP 5 — UPDATE Recommendations table
    -- =========================================
    UPDATE tgt
    SET
        tgt.CurrentEfficiency = src.CurrentEfficiency,
        tgt.WithinEfficiency  = src.WithinEfficiency,
        tgt.LowCpuEfficiency  = src.LowCpuEfficiency
    FROM [Metrics].[MongoDBRightsizingRecommendations] tgt
    INNER JOIN #EffResult src
        ON  tgt.ClusterKey = src.ClusterKey
        AND tgt.Month      = src.MonthName
        AND tgt.DayType    = src.DayType
        AND tgt.HourType   = src.HourType
        AND tgt.CurrentSku = src.CurrentSku;

END
GO


-- =============================================
-- VERIFY
-- =============================================
-- Check SimulatedMetrics table populated
SELECT
    COUNT(*)                   AS TotalRows,
    COUNT(DISTINCT ClusterKey) AS Clusters,
    MIN([Date])                AS DataFrom,
    MAX([Date])                AS DataTo
FROM [Metrics].[MongoDBRightsizingSimulatedMetrics]
GO

-- Check cdr-uat projections
SELECT TOP 5
    ClusterName, [Date], [Hour], DayType, HourType,
    CpuAvg, nCpuAvgWithin, nCpuAvgLowCpu,
    MemAvg, nMemAvgWithin, nMemAvgLowCpu
FROM [Metrics].[MongoDBRightsizingSimulatedMetrics]
WHERE ClusterName = 'cdr-uat'
ORDER BY [Date] DESC, [Hour]
GO

-- Check efficiency columns updated in recommendations
SELECT
    ClusterName, DayType, HourType,
    CurrentEfficiency,
    WithinEfficiency,
    LowCpuEfficiency
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE ClusterName = 'cdr-uat'
GO