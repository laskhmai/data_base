-- -- =============================================
-- Stored Procedure: usp_MongoDBRightsizingAggregatedMetrics5Min
-- Schema  : Metrics
-- Project : MongoDB Rightsizing — COSD Team Humana
-- DevOps  : 9009227
-- Version : 3.0 FINAL
-- Changes from v2:
--   1. ConnUtilizationPct recalculated at cluster level
--      = SUM(all process connections) / ConnectionLimit
--      Gives true cluster-level utilization (e.g. 3.44%)
--      instead of MAX per-process (e.g. 0.37%)
--   2. ConnectionLimit carried through FinalMetrics → ClusterMetrics
-- All previous fixes retained:
--   - #ClusterMetrics aggregation (sharding duplicates fix)
--   - MetaConfig JOIN uses Instance column (not SkuName)
--   - ProcessId/ProcessType/ReplicaSetName removed
-- =============================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROC [Metrics].[usp_MongoDBRightsizingAggregatedMetrics5Min] AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartDT DATE = DATEADD(DAY, -7, CAST(GETDATE() AS DATE));
    DECLARE @EndDT   DATE = DATEADD(DAY, +1, CAST(GETDATE() AS DATE));

    -- =========================================
    -- Drop all temp tables
    -- =========================================
    IF OBJECT_ID('tempdb..#CpuAvg')         IS NOT NULL DROP TABLE #CpuAvg;
    IF OBJECT_ID('tempdb..#CpuMax')         IS NOT NULL DROP TABLE #CpuMax;
    IF OBJECT_ID('tempdb..#MemResident')    IS NOT NULL DROP TABLE #MemResident;
    IF OBJECT_ID('tempdb..#MemAvail')       IS NOT NULL DROP TABLE #MemAvail;
    IF OBJECT_ID('tempdb..#NetIn')          IS NOT NULL DROP TABLE #NetIn;
    IF OBJECT_ID('tempdb..#NetInMax')       IS NOT NULL DROP TABLE #NetInMax;
    IF OBJECT_ID('tempdb..#NetOut')         IS NOT NULL DROP TABLE #NetOut;
    IF OBJECT_ID('tempdb..#NetOutMax')      IS NOT NULL DROP TABLE #NetOutMax;
    IF OBJECT_ID('tempdb..#NetRequests')    IS NOT NULL DROP TABLE #NetRequests;
    IF OBJECT_ID('tempdb..#Conns')          IS NOT NULL DROP TABLE #Conns;
    IF OBJECT_ID('tempdb..#OpcQuery')       IS NOT NULL DROP TABLE #OpcQuery;
    IF OBJECT_ID('tempdb..#OpcInsert')      IS NOT NULL DROP TABLE #OpcInsert;
    IF OBJECT_ID('tempdb..#Keys')           IS NOT NULL DROP TABLE #Keys;
    IF OBJECT_ID('tempdb..#FinalMetrics')   IS NOT NULL DROP TABLE #FinalMetrics;
    IF OBJECT_ID('tempdb..#ClusterMetrics') IS NOT NULL DROP TABLE #ClusterMetrics;

    -- =========================================
    -- 1. CPU AVG + P95
    -- Source : MongoDB_System_Normalized_Cpu_User_5M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket) AS CpuAvg,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY Measurement)
                OVER (PARTITION BY [key], HourBucket)              AS CpuAvgP95,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0))                AS rn
        FROM Raw
    )
    SELECT [key], HourBucket AS DateTimeEST,
           COALESCE(CpuAvg,    0) AS CpuAvg,
           COALESCE(CpuAvgP95, 0) AS CpuAvgP95
    INTO #CpuAvg FROM Win WHERE rn = 1;

    -- =========================================
    -- 2. CPU MAX + P95 + Gt50/25/10
    -- Source : MongoDB_System_Normalized_Cpu_User_Max_5M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_5M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS CpuMax,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY Measurement)
                OVER (PARTITION BY [key], HourBucket)              AS CpuMaxP95,
            SUM(CASE WHEN Measurement > 50 THEN 1 ELSE 0 END)
                OVER (PARTITION BY [key], HourBucket)              AS CpuMaxGt50,
            SUM(CASE WHEN Measurement > 25 THEN 1 ELSE 0 END)
                OVER (PARTITION BY [key], HourBucket)              AS CpuMaxGt25,
            SUM(CASE WHEN Measurement > 10 THEN 1 ELSE 0 END)
                OVER (PARTITION BY [key], HourBucket)              AS CpuMaxGt10,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0))                AS rn
        FROM Raw
    )
    SELECT [key], HourBucket AS DateTimeEST,
           COALESCE(CpuMax,     0) AS CpuMax,
           COALESCE(CpuMaxP95,  0) AS CpuMaxP95,
           COALESCE(CpuMaxGt50, 0) AS CpuMaxGt50,
           COALESCE(CpuMaxGt25, 0) AS CpuMaxGt25,
           COALESCE(CpuMaxGt10, 0) AS CpuMaxGt10
    INTO #CpuMax FROM Win WHERE rn = 1;

    -- =========================================
    -- TRUE CLUSTER P95 — CPU MAX
    -- Combines ALL process readings per cluster
    -- per hour then calculates real P95
    -- =========================================
    IF OBJECT_ID('tempdb..#ClusterCpuP95') IS NOT NULL DROP TABLE #ClusterCpuP95;

    SELECT
        p.ClusterKey,
        DATEADD(HOUR, DATEDIFF(HOUR, 0,
            SWITCHOFFSET(CONVERT(datetimeoffset, m.DateTime), '-05:00')), 0)
            AS DateTimeEST,
        PERCENTILE_CONT(0.95)
            WITHIN GROUP (ORDER BY m.Measurement)    AS CpuMaxP95_True
    INTO #ClusterCpuP95
    FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_5M] m
    JOIN [MongoDB].[Process] p
        ON  p.ProcessId = m.[key]
        AND p.IsDeleted = 0
    WHERE m.DateTime >= @StartDT
    AND   m.DateTime <  @EndDT
    GROUP BY
        p.ClusterKey,
        DATEADD(HOUR, DATEDIFF(HOUR, 0,
            SWITCHOFFSET(CONVERT(datetimeoffset, m.DateTime), '-05:00')), 0);

    -- =========================================
    -- TRUE CLUSTER P95 — CPU AVG
    -- =========================================
    IF OBJECT_ID('tempdb..#ClusterCpuAvgP95') IS NOT NULL DROP TABLE #ClusterCpuAvgP95;

    SELECT
        p.ClusterKey,
        DATEADD(HOUR, DATEDIFF(HOUR, 0,
            SWITCHOFFSET(CONVERT(datetimeoffset, m.DateTime), '-05:00')), 0)
            AS DateTimeEST,
        PERCENTILE_CONT(0.95)
            WITHIN GROUP (ORDER BY m.Measurement)    AS CpuAvgP95_True
    INTO #ClusterCpuAvgP95
    FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M] m
    JOIN [MongoDB].[Process] p
        ON  p.ProcessId = m.[key]
        AND p.IsDeleted = 0
    WHERE m.DateTime >= @StartDT
    AND   m.DateTime <  @EndDT
    GROUP BY
        p.ClusterKey,
        DATEADD(HOUR, DATEDIFF(HOUR, 0,
            SWITCHOFFSET(CONVERT(datetimeoffset, m.DateTime), '-05:00')), 0);

    -- =========================================
    -- 3. MEMORY RESIDENT + P95
    -- Source : MongoDB_Memory_Resident_5M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Memory_Resident_5M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS MemResidentMax,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket) AS MemResidentAvg,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY Measurement)
                OVER (PARTITION BY [key], HourBucket)              AS MemResidentP95,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0))                AS rn
        FROM Raw
    )
    SELECT [key], HourBucket AS DateTimeEST,
           COALESCE(MemResidentMax, 0) AS MemResidentMax,
           COALESCE(MemResidentAvg, 0) AS MemResidentAvg,
           COALESCE(MemResidentP95, 0) AS MemResidentP95
    INTO #MemResident FROM Win WHERE rn = 1;

    -- =========================================
    -- 4. MEMORY AVAILABLE
    -- Source : MongoDB_System_Memory_Available_5M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Memory_Available_5M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MIN(Measurement) OVER (PARTITION BY [key], HourBucket) AS MemAvailableMin,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0))                AS rn
        FROM Raw
    )
    SELECT [key], HourBucket AS DateTimeEST,
           COALESCE(MemAvailableMin, 0) AS MemAvailableMin
    INTO #MemAvail FROM Win WHERE rn = 1;

    -- =========================================
    -- 5. NETWORK IN AVG
    -- Source : MongoDB_System_Network_In_5M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Network_In_5M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket) AS NetInAvg,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0))                AS rn
        FROM Raw
    )
    SELECT [key], HourBucket AS DateTimeEST,
           COALESCE(NetInAvg, 0) AS NetInAvg
    INTO #NetIn FROM Win WHERE rn = 1;

    -- =========================================
    -- 6. NETWORK IN MAX
    -- Source : MongoDB_System_Network_In_Max_5M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Network_In_Max_5M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS NetInMax,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0))                AS rn
        FROM Raw
    )
    SELECT [key], HourBucket AS DateTimeEST,
           COALESCE(NetInMax, 0) AS NetInMax
    INTO #NetInMax FROM Win WHERE rn = 1;

    -- =========================================
    -- 7. NETWORK OUT AVG
    -- Source : MongoDB_System_Network_Out_5M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Network_Out_5M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket) AS NetOutAvg,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0))                AS rn
        FROM Raw
    )
    SELECT [key], HourBucket AS DateTimeEST,
           COALESCE(NetOutAvg, 0) AS NetOutAvg
    INTO #NetOut FROM Win WHERE rn = 1;

    -- =========================================
    -- 8. NETWORK OUT MAX
    -- Source : MongoDB_System_Network_Out_Max_5M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Network_Out_Max_5M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS NetOutMax,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0))                AS rn
        FROM Raw
    )
    SELECT [key], HourBucket AS DateTimeEST,
           COALESCE(NetOutMax, 0) AS NetOutMax
    INTO #NetOutMax FROM Win WHERE rn = 1;

    -- =========================================
    -- 9. NETWORK NUM REQUESTS
    -- Source : MongoDB_Network_Num_Requests_5M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Network_Num_Requests_5M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS NetRequestsMax,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0))                AS rn
        FROM Raw
    )
    SELECT [key], HourBucket AS DateTimeEST,
           COALESCE(NetRequestsMax, 0) AS NetRequestsMax
    INTO #NetRequests FROM Win WHERE rn = 1;

    -- =========================================
    -- 10. CONNECTIONS — 15M (no 5M available)
    -- Source : MongoDB_Connections_15M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Connections_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS ConnectionsMax,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket) AS ConnectionsAvg,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0))                AS rn
        FROM Raw
    )
    SELECT [key], HourBucket AS DateTimeEST,
           COALESCE(ConnectionsMax, 0) AS ConnectionsMax,
           COALESCE(ConnectionsAvg, 0) AS ConnectionsAvg
    INTO #Conns FROM Win WHERE rn = 1;

    -- =========================================
    -- 11. OPCOUNTER QUERY — 15M (no 5M available)
    -- Source : MongoDB_Opcounter_Query_15M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Opcounter_Query_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS OpcQueryMax,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0))                AS rn
        FROM Raw
    )
    SELECT [key], HourBucket AS DateTimeEST,
           COALESCE(OpcQueryMax, 0) AS OpcQueryMax
    INTO #OpcQuery FROM Win WHERE rn = 1;

    -- =========================================
    -- 12. OPCOUNTER INSERT — 15M (no 5M available)
    -- Source : MongoDB_Opcounter_Insert_15M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Opcounter_Insert_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS OpcInsertMax,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0))                AS rn
        FROM Raw
    )
    SELECT [key], HourBucket AS DateTimeEST,
           COALESCE(OpcInsertMax, 0) AS OpcInsertMax
    INTO #OpcInsert FROM Win WHERE rn = 1;

    -- =========================================
    -- Keys spine — UNION all metric keys
    -- =========================================
    SELECT [key], DateTimeEST INTO #Keys FROM #CpuAvg
    UNION SELECT [key], DateTimeEST FROM #CpuMax
    UNION SELECT [key], DateTimeEST FROM #MemResident
    UNION SELECT [key], DateTimeEST FROM #MemAvail
    UNION SELECT [key], DateTimeEST FROM #NetIn
    UNION SELECT [key], DateTimeEST FROM #NetInMax
    UNION SELECT [key], DateTimeEST FROM #NetOut
    UNION SELECT [key], DateTimeEST FROM #NetOutMax
    UNION SELECT [key], DateTimeEST FROM #NetRequests
    UNION SELECT [key], DateTimeEST FROM #Conns
    UNION SELECT [key], DateTimeEST FROM #OpcQuery
    UNION SELECT [key], DateTimeEST FROM #OpcInsert;

    -- =========================================
    -- STEP 1 — Final Assembly per process
    -- NOTE: ConnectionLimit carried through for
    --       cluster-level ConnUtilizationPct calc
    -- =========================================
    SELECT
        p.ClusterKey,
        cl.Name                                                             AS ClusterName,
        p.ProjectKey,
        p.OrgKey,
        COALESCE(
            JSON_VALUE(cl.ReplicationSpecs,
                '$[0].regionConfigs[0].effectiveElectableSpecs.instanceSize'),
            JSON_VALUE(cl.ReplicationSpecs,
                '$[0].regionConfigs[0].electableSpecs.instanceSize')
        )                                                                   AS InstanceSize,
        JSON_VALUE(cl.ReplicationSpecs,
            '$[0].regionConfigs[0].providerName')                           AS ProviderName,
        JSON_VALUE(cl.ReplicationSpecs,
            '$[0].regionConfigs[0].regionName')                             AS RegionName,
        k.DateTimeEST,
        CAST(k.DateTimeEST AS DATE)                                         AS _date,
        DATEPART(HOUR, k.DateTimeEST)                                       AS _hour,
        CASE WHEN DATEPART(WEEKDAY, k.DateTimeEST) IN (1,7)
             THEN 'Weekend' ELSE 'Weekday' END                              AS [type],
        CASE WHEN DATEPART(HOUR, k.DateTimeEST) BETWEEN 7 AND 18
             THEN 'BusinessHours' ELSE 'NonBusinessHours' END               AS businessHour,

        -- CPU
        COALESCE(ca.CpuAvg,          0)                                     AS CpuAvg,
        COALESCE(ca.CpuAvgP95,       0)                                     AS CpuAvgP95,
        COALESCE(cm.CpuMax,          0)                                     AS CpuMax,
        COALESCE(cm.CpuMaxP95,       0)                                     AS CpuMaxP95,
        COALESCE(cm.CpuMaxGt50,      0)                                     AS CpuMaxGt50,
        COALESCE(cm.CpuMaxGt25,      0)                                     AS CpuMaxGt25,
        COALESCE(cm.CpuMaxGt10,      0)                                     AS CpuMaxGt10,

        -- Memory RAW
        COALESCE(mr.MemResidentMax,  0)                                     AS MemResidentMax,
        COALESCE(mr.MemResidentAvg,  0)                                     AS MemResidentAvg,
        COALESCE(ma.MemAvailableMin, 0)                                     AS MemAvailableMin,

        -- Memory % — per process using MetaConfig.MemorySizeGB
        CASE WHEN COALESCE(m.MemorySizeGB, 0) > 0
            THEN ROUND((COALESCE(mr.MemResidentMax, 0)
                 / (m.MemorySizeGB * 1024)) * 100, 2) ELSE 0
        END                                                                 AS MemResidentMaxPct,
        CASE WHEN COALESCE(m.MemorySizeGB, 0) > 0
            THEN ROUND((COALESCE(mr.MemResidentAvg, 0)
                 / (m.MemorySizeGB * 1024)) * 100, 2) ELSE 0
        END                                                                 AS MemResidentAvgPct,
        CASE WHEN COALESCE(m.MemorySizeGB, 0) > 0
            THEN ROUND((COALESCE(mr.MemResidentP95, 0)
                 / (m.MemorySizeGB * 1024)) * 100, 2) ELSE 0
        END                                                                 AS MemResidentP95Pct,

        -- Network
        COALESCE(ni.NetInAvg,        0)                                     AS NetInAvg,
        COALESCE(nim.NetInMax,       0)                                     AS NetInMax,
        COALESCE(no2.NetOutAvg,      0)                                     AS NetOutAvg,
        COALESCE(nom.NetOutMax,      0)                                     AS NetOutMax,
        COALESCE(nr.NetRequestsMax,  0)                                     AS NetRequestsMax,

        -- Connections RAW
        COALESCE(cn.ConnectionsMax,  0)                                     AS ConnectionsMax,
        COALESCE(cn.ConnectionsAvg,  0)                                     AS ConnectionsAvg,

        -- ConnectionLimit carried through for cluster-level calc in Step 2
        COALESCE(m.ConnectionLimit,  0)                                     AS ConnectionLimit,

        -- Ops
        COALESCE(oq.OpcQueryMax,     0)                                     AS OpcQueryMax,
        COALESCE(oi.OpcInsertMax,    0)                                     AS OpcInsertMax

    INTO #FinalMetrics
    FROM  #Keys                  k
    JOIN  [MongoDB].[Process]    p
        ON  p.ProcessId    = k.[key]
        AND p.IsDeleted    = 0
    JOIN  [MongoDB].[Clusters]   cl
        ON  cl.ClustersKey = p.ClusterKey
    LEFT JOIN [Analytics].[MongoDBMetaConfig] m
        ON  m.Instance = COALESCE(
                JSON_VALUE(cl.ReplicationSpecs,
                    '$[0].regionConfigs[0].effectiveElectableSpecs.instanceSize'),
                JSON_VALUE(cl.ReplicationSpecs,
                    '$[0].regionConfigs[0].electableSpecs.instanceSize'))
        AND m.Provider = JSON_VALUE(cl.ReplicationSpecs,
                            '$[0].regionConfigs[0].providerName')
        AND m.Region   = JSON_VALUE(cl.ReplicationSpecs,
                            '$[0].regionConfigs[0].regionName')
        AND m.Tier NOT IN ('Free','Flex')
    LEFT JOIN #CpuAvg      ca   ON  ca.[key]  = k.[key] AND ca.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #CpuMax      cm   ON  cm.[key]  = k.[key] AND cm.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #MemResident mr   ON  mr.[key]  = k.[key] AND mr.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #MemAvail    ma   ON  ma.[key]  = k.[key] AND ma.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #NetIn       ni   ON  ni.[key]  = k.[key] AND ni.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #NetInMax    nim  ON  nim.[key] = k.[key] AND nim.DateTimeEST = k.DateTimeEST
    LEFT JOIN #NetOut      no2  ON  no2.[key] = k.[key] AND no2.DateTimeEST = k.DateTimeEST
    LEFT JOIN #NetOutMax   nom  ON  nom.[key] = k.[key] AND nom.DateTimeEST = k.DateTimeEST
    LEFT JOIN #NetRequests nr   ON  nr.[key]  = k.[key] AND nr.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #Conns       cn   ON  cn.[key]  = k.[key] AND cn.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #OpcQuery    oq   ON  oq.[key]  = k.[key] AND oq.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #OpcInsert   oi   ON  oi.[key]  = k.[key] AND oi.DateTimeEST  = k.DateTimeEST;

    -- =========================================
    -- STEP 2 — Cluster Level Aggregation
    -- Collapses all shard processes → 1 row per cluster per hour
    -- ConnUtilizationPct calculated HERE using:
    --   SUM(all process connections) / ConnectionLimit
    --   = true cluster-level connection utilization
    --   Example: 2,200 / 64,000 = 3.44%
    -- =========================================
    SELECT
        ClusterKey,
        ClusterName,
        MAX(InstanceSize)       AS InstanceSize,
        MAX(ProviderName)       AS ProviderName,
        MAX(RegionName)         AS RegionName,
        MAX(ProjectKey)         AS ProjectKey,
        MAX(OrgKey)             AS OrgKey,
        DateTimeEST,
        _date,
        _hour,
        [type],
        businessHour,

        -- CPU — worst process drives sizing
        AVG(CpuAvg)             AS CpuAvg,
        AVG(CpuAvgP95)          AS CpuAvgP95,
        MAX(CpuMax)             AS CpuMax,
        AVG(CpuMaxP95)          AS CpuMaxP95,    -- FIXED: AVG not MAX (true cluster P95)
        SUM(CpuMaxGt50)         AS CpuMaxGt50,
        SUM(CpuMaxGt25)         AS CpuMaxGt25,
        SUM(CpuMaxGt10)         AS CpuMaxGt10,

        -- Memory — worst process drives sizing
        MAX(MemResidentMax)     AS MemResidentMax,
        AVG(MemResidentAvg)     AS MemResidentAvg,
        MIN(MemAvailableMin)    AS MemAvailableMin,  -- FIXED: MIN not MAX (worst shard)
        MAX(MemResidentMaxPct)  AS MemResidentMaxPct,
        AVG(MemResidentAvgPct)  AS MemResidentAvgPct,
        AVG(MemResidentP95Pct)  AS MemResidentP95Pct,  -- FIXED: AVG not MAX (true cluster P95)

        -- Network
        AVG(NetInAvg)           AS NetInAvg,
        MAX(NetInMax)           AS NetInMax,
        AVG(NetOutAvg)          AS NetOutAvg,
        MAX(NetOutMax)          AS NetOutMax,
        MAX(NetRequestsMax)     AS NetRequestsMax,

        -- Connections — SUM all processes = total cluster connections
        SUM(ConnectionsMax)     AS ConnectionsMax,
        SUM(ConnectionsAvg)     AS ConnectionsAvg,

        -- ConnUtilizationPct — calculated at cluster level (v3 fix)
        -- SUM(all process connections) / ConnectionLimit
        -- gives true cluster utilization e.g. 2,200/64,000 = 3.44%
        CASE WHEN MAX(ConnectionLimit) > 0
            THEN ROUND((SUM(ConnectionsMax) / MAX(ConnectionLimit)) * 100, 2)
            ELSE 0
        END                     AS ConnUtilizationPct,

        -- Ops
        MAX(OpcQueryMax)        AS OpcQueryMax,
        MAX(OpcInsertMax)       AS OpcInsertMax,

        -- ProcessKey columns (added v4 — for validation and simulated metrics)
        MAX(CASE WHEN CpuMax = (
                SELECT MAX(CpuMax) FROM #FinalMetrics f2
                WHERE f2.ClusterKey  = f.ClusterKey
                AND   f2.DateTimeEST = f.DateTimeEST)
            THEN ProcessId END)   AS MaxCpuProcessId,
        MAX(CASE WHEN MemResidentMax = (
                SELECT MAX(MemResidentMax) FROM #FinalMetrics f2
                WHERE f2.ClusterKey  = f.ClusterKey
                AND   f2.DateTimeEST = f.DateTimeEST)
            THEN ProcessId END)   AS MaxMemProcessId

    INTO #ClusterMetrics
    FROM #FinalMetrics f
    GROUP BY
        ClusterKey, ClusterName,
        DateTimeEST, _date, _hour, [type], businessHour;

    -- Update with TRUE cluster P95 values
    UPDATE cm
    SET
        cm.CpuMaxP95 = cp.CpuMaxP95_True
    FROM #ClusterMetrics cm
    JOIN #ClusterCpuP95  cp
        ON  cp.ClusterKey  = cm.ClusterKey
        AND cp.DateTimeEST = cm.DateTimeEST;

    UPDATE cm
    SET
        cm.CpuAvgP95 = ca.CpuAvgP95_True
    FROM #ClusterMetrics cm
    JOIN #ClusterCpuAvgP95 ca
        ON  ca.ClusterKey  = cm.ClusterKey
        AND ca.DateTimeEST = cm.DateTimeEST;

    -- =========================================
    -- UPSERT — UPDATE existing rows
    -- Match key: ClusterKey
    -- =========================================
    UPDATE T
    SET
        T.ClusterName           = S.ClusterName,
        T.InstanceSize          = S.InstanceSize,
        T.ProviderName          = S.ProviderName,
        T.RegionName            = S.RegionName,
        T.ProjectKey            = S.ProjectKey,
        T.OrgKey                = S.OrgKey,
        T.CpuAvg                = S.CpuAvg,
        T.CpuAvgP95             = S.CpuAvgP95,
        T.CpuMax                = S.CpuMax,
        T.CpuMaxP95             = S.CpuMaxP95,
        T.CpuMaxGt50            = S.CpuMaxGt50,
        T.CpuMaxGt25            = S.CpuMaxGt25,
        T.CpuMaxGt10            = S.CpuMaxGt10,
        T.MemResidentMax        = S.MemResidentMax,
        T.MemResidentAvg        = S.MemResidentAvg,
        T.MemAvailableMin       = S.MemAvailableMin,
        T.MemResidentMaxPct     = S.MemResidentMaxPct,
        T.MemResidentAvgPct     = S.MemResidentAvgPct,
        T.MemResidentP95Pct     = S.MemResidentP95Pct,
        T.NetInAvg              = S.NetInAvg,
        T.NetInMax              = S.NetInMax,
        T.NetOutAvg             = S.NetOutAvg,
        T.NetOutMax             = S.NetOutMax,
        T.NetRequestsMax        = S.NetRequestsMax,
        T.ConnectionsMax        = S.ConnectionsMax,
        T.ConnectionsAvg        = S.ConnectionsAvg,
        T.ConnUtilizationPct    = S.ConnUtilizationPct,
        T.OpcQueryMax           = S.OpcQueryMax,
        T.OpcInsertMax          = S.OpcInsertMax,
        T.MaxCpuProcessId       = S.MaxCpuProcessId,
        T.MaxMemProcessId       = S.MaxMemProcessId
    FROM [Metrics].[MongoDBRightsizingAggregated5Min] T
    JOIN #ClusterMetrics S
        ON  T.ClusterKey   = S.ClusterKey
        AND T.DateTimeEST  = S.DateTimeEST
        AND T._date        = S._date
        AND T._hour        = S._hour
        AND T.[type]       = S.[type]
        AND T.businessHour = S.businessHour;

    -- =========================================
    -- UPSERT — INSERT new rows
    -- =========================================
    INSERT INTO [Metrics].[MongoDBRightsizingAggregated5Min]
    (
        ClusterKey, ClusterName,
        InstanceSize, ProviderName, RegionName,
        ProjectKey, OrgKey,
        DateTimeEST, _date, _hour, [type], businessHour,
        CpuAvg, CpuAvgP95, CpuMax, CpuMaxP95,
        CpuMaxGt50, CpuMaxGt25, CpuMaxGt10,
        MemResidentMax, MemResidentAvg, MemAvailableMin,
        MemResidentMaxPct, MemResidentAvgPct, MemResidentP95Pct,
        NetInAvg, NetInMax, NetOutAvg, NetOutMax, NetRequestsMax,
        ConnectionsMax, ConnectionsAvg, ConnUtilizationPct,
        OpcQueryMax, OpcInsertMax,
        MaxCpuProcessId, MaxMemProcessId
    )
    SELECT
        S.ClusterKey, S.ClusterName,
        S.InstanceSize, S.ProviderName, S.RegionName,
        S.ProjectKey, S.OrgKey,
        S.DateTimeEST, S._date, S._hour, S.[type], S.businessHour,
        S.CpuAvg, S.CpuAvgP95, S.CpuMax, S.CpuMaxP95,
        S.CpuMaxGt50, S.CpuMaxGt25, S.CpuMaxGt10,
        S.MemResidentMax, S.MemResidentAvg, S.MemAvailableMin,
        S.MemResidentMaxPct, S.MemResidentAvgPct, S.MemResidentP95Pct,
        S.NetInAvg, S.NetInMax, S.NetOutAvg, S.NetOutMax, S.NetRequestsMax,
        S.ConnectionsMax, S.ConnectionsAvg, S.ConnUtilizationPct,
        S.OpcQueryMax, S.OpcInsertMax,
        S.MaxCpuProcessId, S.MaxMemProcessId
    FROM #ClusterMetrics S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [Metrics].[MongoDBRightsizingAggregated5Min] T
        WHERE T.ClusterKey   = S.ClusterKey
        AND   T.DateTimeEST  = S.DateTimeEST
        AND   T._date        = S._date
        AND   T._hour        = S._hour
        AND   T.[type]       = S.[type]
        AND   T.businessHour = S.businessHour
    );

END
GO

-- =============================================
-- DEPLOY STEPS
-- =============================================
-- Step 1: TRUNCATE TABLE [Metrics].[MongoDBRightsizingAggregated5Min];
-- Step 2: Run CREATE OR ALTER PROC above
-- Step 3: EXEC [Metrics].[usp_MongoDBRightsizingAggregatedMetrics5Min]
-- Step 4: Run verify queries below

-- =============================================
-- VERIFY
-- =============================================

-- Row counts
SELECT
    COUNT(*)                   AS TotalRows,
    COUNT(DISTINCT ClusterKey) AS Clusters,
    MIN(DateTimeEST)           AS MinDate,
    MAX(DateTimeEST)           AS MaxDate
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GO

-- No duplicates — must return ZERO rows
SELECT ClusterKey, ClusterName, _date, _hour, COUNT(*) AS RowCount
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GROUP BY ClusterKey, ClusterName, _date, _hour
HAVING COUNT(*) > 1
ORDER BY RowCount DESC
GO

-- cdr-uat spot check
-- ConnUtilizationPct should now show ~3.44% (2200/64000)
SELECT
    ClusterKey, ClusterName,
    _date, _hour,
    CpuMax,
    MemResidentMaxPct,
    ConnectionsMax,
    ConnUtilizationPct
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterKey = 330
ORDER BY _date, _hour
GO
-- -- Save current values before update
SELECT
    ClusterName,
    _date,
    _hour,
    ROUND(CpuMaxP95,        2) AS CpuMaxP95_Current,
    ROUND(CpuAvgP95,        2) AS CpuAvgP95_Current,
    ROUND(MemResidentP95Pct,2) AS MemP95_Current,
    ROUND(MemAvailableMin,  0) AS MemAvailableMin_Current
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'cdr-uat'      -- change cluster
AND   _date       = '2026-05-22'   -- change date
ORDER BY _hour
GO


-- 1. Add new columns first
ALTER TABLE [Metrics].[MongoDBRightsizingAggregated5Min]
ADD MaxCpuProcessId NVARCHAR(500) NULL,
    MaxMemProcessId NVARCHAR(500) NULL
GO

-- 2. Run v4 proc
EXEC [Metrics].[usp_MongoDBRightsizingAggregatedMetrics5Min]
GO

-- Compare new values vs old
SELECT
    ClusterName,
    _date,
    _hour,
    ROUND(CpuMaxP95,        2) AS CpuMaxP95_New,
    ROUND(CpuAvgP95,        2) AS CpuAvgP95_New,
    ROUND(MemResidentP95Pct,2) AS MemP95_New,
    ROUND(MemAvailableMin,  0) AS MemAvailableMin_New,
    MaxCpuProcessId,
    MaxMemProcessId
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'cdr-uat'      -- same cluster
AND   _date       = '2026-05-22'   -- same date
ORDER BY _hour
GO