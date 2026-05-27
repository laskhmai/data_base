-- =============================================
-- Stored Procedure: usp_MongoDBRightsizingAggregatedMetrics
-- Schema  : Metrics
-- Project : MongoDB Rightsizing — COSD Team Humana
-- DevOps  : 9009227
-- Version : 2.0
-- Changes :
--   v2.0 - Added P95 for CPU and Memory
--         - Added Memory % calculation from MetaConfig
--         - Added Connection % calculation from MetaConfig
--         - JOIN to MetaConfig for MemorySizeGB + ConnectionLimit
-- =============================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [Metrics].[usp_MongoDBRightsizingAggregatedMetrics] AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartDT DATE = DATEADD(DAY, -7, CAST(GETDATE() AS DATE));
    DECLARE @EndDT   DATE = DATEADD(DAY, +1, CAST(GETDATE() AS DATE));

    -- =========================================
    -- Drop all temp tables
    -- =========================================
    IF OBJECT_ID('tempdb..#CpuAvg')       IS NOT NULL DROP TABLE #CpuAvg;
    IF OBJECT_ID('tempdb..#CpuMax')       IS NOT NULL DROP TABLE #CpuMax;
    IF OBJECT_ID('tempdb..#MemResident')  IS NOT NULL DROP TABLE #MemResident;
    IF OBJECT_ID('tempdb..#MemAvail')     IS NOT NULL DROP TABLE #MemAvail;
    IF OBJECT_ID('tempdb..#NetIn')        IS NOT NULL DROP TABLE #NetIn;
    IF OBJECT_ID('tempdb..#NetInMax')     IS NOT NULL DROP TABLE #NetInMax;
    IF OBJECT_ID('tempdb..#NetOut')       IS NOT NULL DROP TABLE #NetOut;
    IF OBJECT_ID('tempdb..#NetOutMax')    IS NOT NULL DROP TABLE #NetOutMax;
    IF OBJECT_ID('tempdb..#NetRequests')  IS NOT NULL DROP TABLE #NetRequests;
    IF OBJECT_ID('tempdb..#Conns')        IS NOT NULL DROP TABLE #Conns;
    IF OBJECT_ID('tempdb..#OpcQuery')     IS NOT NULL DROP TABLE #OpcQuery;
    IF OBJECT_ID('tempdb..#OpcInsert')    IS NOT NULL DROP TABLE #OpcInsert;
    IF OBJECT_ID('tempdb..#Keys')         IS NOT NULL DROP TABLE #Keys;
    IF OBJECT_ID('tempdb..#FinalMetrics') IS NOT NULL DROP TABLE #FinalMetrics;

    -- =========================================
    -- 1. CPU AVG + CpuAvgP95
    -- Source: MongoDB_System_Normalized_Cpu_User_15M
    -- Unit  : Percent normalized across cores
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS CpuAvg,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY Measurement)
                OVER (PARTITION BY [key], HourBucket)
                AS CpuAvgP95,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0)) AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket              AS DateTimeEST,
        COALESCE(CpuAvg,    0)  AS CpuAvg,
        COALESCE(CpuAvgP95, 0)  AS CpuAvgP95
    INTO #CpuAvg
    FROM Win WHERE rn = 1;

    -- =========================================
    -- 2. CPU MAX + CpuMaxP95 + Gt50/25/10
    -- Source: MongoDB_System_Normalized_Cpu_User_Max_15M
    -- Unit  : Percent normalized across cores
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS CpuMax,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY Measurement)
                OVER (PARTITION BY [key], HourBucket)
                AS CpuMaxP95,
            SUM(CASE WHEN Measurement > 50 THEN 1 ELSE 0 END)
                OVER (PARTITION BY [key], HourBucket) AS CpuMaxGt50,
            SUM(CASE WHEN Measurement > 25 THEN 1 ELSE 0 END)
                OVER (PARTITION BY [key], HourBucket) AS CpuMaxGt25,
            SUM(CASE WHEN Measurement > 10 THEN 1 ELSE 0 END)
                OVER (PARTITION BY [key], HourBucket) AS CpuMaxGt10,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0)) AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                  AS DateTimeEST,
        COALESCE(CpuMax,     0)     AS CpuMax,
        COALESCE(CpuMaxP95,  0)     AS CpuMaxP95,
        COALESCE(CpuMaxGt50, 0)     AS CpuMaxGt50,
        COALESCE(CpuMaxGt25, 0)     AS CpuMaxGt25,
        COALESCE(CpuMaxGt10, 0)     AS CpuMaxGt10
    INTO #CpuMax
    FROM Win WHERE rn = 1;

    -- =========================================
    -- 3. MEMORY RESIDENT + MemResidentP95
    -- Source: MongoDB_Memory_Resident_5M
    -- NOTE  : Using 5M — 15M is DISABLED
    -- Unit  : MB
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
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS MemResidentMax,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS MemResidentAvg,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY Measurement)
                OVER (PARTITION BY [key], HourBucket)
                AS MemResidentP95,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0)) AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                      AS DateTimeEST,
        COALESCE(MemResidentMax, 0)     AS MemResidentMax,
        COALESCE(MemResidentAvg, 0)     AS MemResidentAvg,
        COALESCE(MemResidentP95, 0)     AS MemResidentP95
    INTO #MemResident
    FROM Win WHERE rn = 1;

    -- =========================================
    -- 4. MEMORY AVAILABLE
    -- Source: MongoDB_System_Memory_Available_15M
    -- Unit  : KB — MIN = worst case free memory
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Memory_Available_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MIN(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS MemAvailableMin,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0)) AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                      AS DateTimeEST,
        COALESCE(MemAvailableMin, 0)    AS MemAvailableMin
    INTO #MemAvail
    FROM Win WHERE rn = 1;

    -- =========================================
    -- 5. NETWORK IN AVG
    -- Source: MongoDB_System_Network_In_15M
    -- Unit  : BytesPerSec
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Network_In_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS NetInAvg,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0)) AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket              AS DateTimeEST,
        COALESCE(NetInAvg, 0)   AS NetInAvg
    INTO #NetIn
    FROM Win WHERE rn = 1;

    -- =========================================
    -- 6. NETWORK IN MAX
    -- Source: MongoDB_System_Network_In_Max_15M
    -- Unit  : BytesPerSec
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Network_In_Max_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS NetInMax,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0)) AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket              AS DateTimeEST,
        COALESCE(NetInMax, 0)   AS NetInMax
    INTO #NetInMax
    FROM Win WHERE rn = 1;

    -- =========================================
    -- 7. NETWORK OUT AVG
    -- Source: MongoDB_System_Network_Out_15M
    -- Unit  : BytesPerSec
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Network_Out_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS NetOutAvg,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0)) AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket              AS DateTimeEST,
        COALESCE(NetOutAvg, 0)  AS NetOutAvg
    INTO #NetOut
    FROM Win WHERE rn = 1;

    -- =========================================
    -- 8. NETWORK OUT MAX
    -- Source: MongoDB_System_Network_Out_Max_15M
    -- Unit  : BytesPerSec
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Network_Out_Max_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS NetOutMax,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0)) AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket              AS DateTimeEST,
        COALESCE(NetOutMax, 0)  AS NetOutMax
    INTO #NetOutMax
    FROM Win WHERE rn = 1;

    -- =========================================
    -- 9. NETWORK NUM REQUESTS
    -- Source: MongoDB_Network_Num_Requests_15M
    -- Unit  : Scalar_P (requests per second)
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Network_Num_Requests_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS NetRequestsMax,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0)) AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                  AS DateTimeEST,
        COALESCE(NetRequestsMax, 0) AS NetRequestsMax
    INTO #NetRequests
    FROM Win WHERE rn = 1;

    -- =========================================
    -- 10. CONNECTIONS
    -- Source: MongoDB_Connections_15M
    -- Unit  : Count per process
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
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS ConnectionsMax,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS ConnectionsAvg,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0)) AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                  AS DateTimeEST,
        COALESCE(ConnectionsMax, 0) AS ConnectionsMax,
        COALESCE(ConnectionsAvg, 0) AS ConnectionsAvg
    INTO #Conns
    FROM Win WHERE rn = 1;

    -- =========================================
    -- 11. OPCOUNTER QUERY
    -- Source: MongoDB_Opcounter_Query_15M
    -- Unit  : Scalar_P ops/sec
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
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS OpcQueryMax,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0)) AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket              AS DateTimeEST,
        COALESCE(OpcQueryMax,0) AS OpcQueryMax
    INTO #OpcQuery
    FROM Win WHERE rn = 1;

    -- =========================================
    -- 12. OPCOUNTER INSERT
    -- Source: MongoDB_Opcounter_Insert_15M
    -- Unit  : Scalar_P ops/sec
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
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS OpcInsertMax,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0)) AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                AS DateTimeEST,
        COALESCE(OpcInsertMax, 0) AS OpcInsertMax
    INTO #OpcInsert
    FROM Win WHERE rn = 1;

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
    -- Final Assembly
    -- Flow: Metric → Process → Clusters → MetaConfig
    -- MetaConfig used for:
    --   MemorySizeGB    → Memory % calculation
    --   ConnectionLimit → Connection % calculation
    -- =========================================
    SELECT
        -- Identity
        p.ClusterKey,
        cl.Name                                                             AS ClusterName,
        k.[key]                                                             AS ProcessId,
        p.ProcessType,
        p.ReplicaSetName,
        p.ProjectKey,
        p.OrgKey,

        -- SKU — COALESCE both JSON paths
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

        -- Time
        k.DateTimeEST,
        CAST(k.DateTimeEST AS DATE)                                         AS _date,
        DATEPART(HOUR, k.DateTimeEST)                                       AS _hour,
        CASE WHEN DATEPART(WEEKDAY, k.DateTimeEST) IN (1,7)
             THEN 'Weekend' ELSE 'Weekday' END                              AS [type],
        CASE WHEN DATEPART(HOUR, k.DateTimeEST) BETWEEN 7 AND 18
             THEN 'BusinessHours' ELSE 'NonBusinessHours' END               AS businessHour,

        -- CPU
        COALESCE(ca.CpuAvg,     0)                                          AS CpuAvg,
        COALESCE(ca.CpuAvgP95,  0)                                          AS CpuAvgP95,
        COALESCE(cm.CpuMax,     0)                                          AS CpuMax,
        COALESCE(cm.CpuMaxP95,  0)                                          AS CpuMaxP95,
        COALESCE(cm.CpuMaxGt50, 0)                                          AS CpuMaxGt50,
        COALESCE(cm.CpuMaxGt25, 0)                                          AS CpuMaxGt25,
        COALESCE(cm.CpuMaxGt10, 0)                                          AS CpuMaxGt10,

        -- Memory RAW (MB)
        COALESCE(mr.MemResidentMax,  0)                                     AS MemResidentMax,
        COALESCE(mr.MemResidentAvg,  0)                                     AS MemResidentAvg,
        COALESCE(mr.MemResidentP95,  0)                                     AS MemResidentP95,
        COALESCE(ma.MemAvailableMin, 0)                                     AS MemAvailableMin,

        -- Memory PERCENTAGE
        -- Flow: MemResidentMax(MB) / (MetaConfig.MemorySizeGB × 1024) × 100
        CASE
            WHEN COALESCE(m.MemorySizeGB, 0) > 0
            THEN ROUND((COALESCE(mr.MemResidentMax, 0)
                 / (m.MemorySizeGB * 1024)) * 100, 2)
            ELSE 0
        END                                                                 AS MemResidentMaxPct,

        CASE
            WHEN COALESCE(m.MemorySizeGB, 0) > 0
            THEN ROUND((COALESCE(mr.MemResidentAvg, 0)
                 / (m.MemorySizeGB * 1024)) * 100, 2)
            ELSE 0
        END                                                                 AS MemResidentAvgPct,

        CASE
            WHEN COALESCE(m.MemorySizeGB, 0) > 0
            THEN ROUND((COALESCE(mr.MemResidentP95, 0)
                 / (m.MemorySizeGB * 1024)) * 100, 2)
            ELSE 0
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

        -- Connection PERCENTAGE
        -- Flow: ConnectionsMax / MetaConfig.ConnectionLimit × 100
        CASE
            WHEN COALESCE(m.ConnectionLimit, 0) > 0
            THEN ROUND((COALESCE(cn.ConnectionsMax, 0)
                 / m.ConnectionLimit) * 100, 2)
            ELSE 0
        END                                                                 AS ConnUtilizationPct,

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

    -- MetaConfig JOIN for memory % and connection %
    LEFT JOIN [Analytics].[MongoDBMetaConfig] m
        ON  m.SkuName  = COALESCE(
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
    -- UPSERT — UPDATE existing rows
    -- =========================================
    UPDATE T
    SET
        T.ClusterKey            = S.ClusterKey,
        T.ClusterName           = S.ClusterName,
        T.ProcessType           = S.ProcessType,
        T.ReplicaSetName        = S.ReplicaSetName,
        T.ProjectKey            = S.ProjectKey,
        T.OrgKey                = S.OrgKey,
        T.InstanceSize          = S.InstanceSize,
        T.ProviderName          = S.ProviderName,
        T.RegionName            = S.RegionName,
        -- CPU
        T.CpuAvg                = S.CpuAvg,
        T.CpuAvgP95             = S.CpuAvgP95,
        T.CpuMax                = S.CpuMax,
        T.CpuMaxP95             = S.CpuMaxP95,
        T.CpuMaxGt50            = S.CpuMaxGt50,
        T.CpuMaxGt25            = S.CpuMaxGt25,
        T.CpuMaxGt10            = S.CpuMaxGt10,
        -- Memory RAW
        T.MemResidentMax        = S.MemResidentMax,
        T.MemResidentAvg        = S.MemResidentAvg,
        T.MemResidentP95        = S.MemResidentP95,
        T.MemAvailableMin       = S.MemAvailableMin,
        -- Memory %
        T.MemResidentMaxPct     = S.MemResidentMaxPct,
        T.MemResidentAvgPct     = S.MemResidentAvgPct,
        T.MemResidentP95Pct     = S.MemResidentP95Pct,
        -- Network
        T.NetInAvg              = S.NetInAvg,
        T.NetInMax              = S.NetInMax,
        T.NetOutAvg             = S.NetOutAvg,
        T.NetOutMax             = S.NetOutMax,
        T.NetRequestsMax        = S.NetRequestsMax,
        -- Connections
        T.ConnectionsMax        = S.ConnectionsMax,
        T.ConnectionsAvg        = S.ConnectionsAvg,
        T.ConnUtilizationPct    = S.ConnUtilizationPct,
        -- Ops
        T.OpcQueryMax           = S.OpcQueryMax,
        T.OpcInsertMax          = S.OpcInsertMax
    FROM [Metrics].[MongoDBRightsizingAggregatedHourly] T
    JOIN #FinalMetrics S
        ON  T.ProcessId    = S.ProcessId
        AND T.DateTimeEST  = S.DateTimeEST
        AND T._date        = S._date
        AND T._hour        = S._hour
        AND T.[type]       = S.[type]
        AND T.businessHour = S.businessHour;

    -- =========================================
    -- UPSERT — INSERT new rows
    -- =========================================
    INSERT INTO [Metrics].[MongoDBRightsizingAggregatedHourly]
    (
        ClusterKey, ClusterName, ProcessId, ProcessType, ReplicaSetName,
        ProjectKey, OrgKey,
        InstanceSize, ProviderName, RegionName,
        DateTimeEST, _date, _hour, [type], businessHour,
        -- CPU
        CpuAvg, CpuAvgP95,
        CpuMax, CpuMaxP95,
        CpuMaxGt50, CpuMaxGt25, CpuMaxGt10,
        -- Memory RAW
        MemResidentMax, MemResidentAvg, MemResidentP95, MemAvailableMin,
        -- Memory %
        MemResidentMaxPct, MemResidentAvgPct, MemResidentP95Pct,
        -- Network
        NetInAvg, NetInMax, NetOutAvg, NetOutMax, NetRequestsMax,
        -- Connections
        ConnectionsMax, ConnectionsAvg, ConnUtilizationPct,
        -- Ops
        OpcQueryMax, OpcInsertMax
    )
    SELECT
        S.ClusterKey, S.ClusterName, S.ProcessId, S.ProcessType, S.ReplicaSetName,
        S.ProjectKey, S.OrgKey,
        S.InstanceSize, S.ProviderName, S.RegionName,
        S.DateTimeEST, S._date, S._hour, S.[type], S.businessHour,
        S.CpuAvg, S.CpuAvgP95,
        S.CpuMax, S.CpuMaxP95,
        S.CpuMaxGt50, S.CpuMaxGt25, S.CpuMaxGt10,
        S.MemResidentMax, S.MemResidentAvg, S.MemResidentP95, S.MemAvailableMin,
        S.MemResidentMaxPct, S.MemResidentAvgPct, S.MemResidentP95Pct,
        S.NetInAvg, S.NetInMax, S.NetOutAvg, S.NetOutMax, S.NetRequestsMax,
        S.ConnectionsMax, S.ConnectionsAvg, S.ConnUtilizationPct,
        S.OpcQueryMax, S.OpcInsertMax
    FROM #FinalMetrics S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [Metrics].[MongoDBRightsizingAggregatedHourly] T
        WHERE T.ProcessId    = S.ProcessId
        AND   T.DateTimeEST  = S.DateTimeEST
        AND   T._date        = S._date
        AND   T._hour        = S._hour
        AND   T.[type]       = S.[type]
        AND   T.businessHour = S.businessHour
    );

END
GO

-- =============================================
-- Execute
-- =============================================
EXEC [Metrics].[usp_MongoDBRightsizingAggregatedMetrics]
GO

-- =============================================
-- Verify new columns populated
-- =============================================
SELECT TOP 10
    ClusterName,
    InstanceSize,
    CpuAvg,
    CpuAvgP95,
    CpuMax,
    CpuMaxP95,
    MemResidentMax,
    MemResidentMaxPct,
    MemResidentAvgPct,
    MemResidentP95Pct,
    ConnectionsMax,
    ConnUtilizationPct
FROM [Metrics].[MongoDBRightsizingAggregatedHourly]
WHERE ProcessType = 'REPLICA_PRIMARY'
ORDER BY DateTimeEST DESC
GO