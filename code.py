-- =============================================
-- MongoDB Rightsizing — 5 Minute Version
-- Project : MongoDB Rightsizing — COSD Team Humana
-- DevOps  : 9009227
-- Contents:
--   1. CREATE TABLE  MongoDBRightsizingAggregated5Min
--   2. CREATE PROC   usp_MongoDBRightsizingAggregatedMetrics5Min
--   3. EXECUTE + VERIFY
-- =============================================


-- =============================================
-- SECTION 1: CREATE TABLE
-- MongoDBRightsizingAggregated5Min
-- Same structure as MongoDBRightsizingAggregatedHourly
-- Uses 5M source tables — 12 readings per hour
-- More accurate P95 and Gt50/25/10 counts
-- =============================================

CREATE TABLE [Metrics].[MongoDBRightsizingAggregated5Min]
(
    -- Identity
    Id                  BIGINT          IDENTITY(1,1)   NOT NULL,
    ClusterKey          INT             NULL,
    ClusterName         NVARCHAR(255)   NULL,
    ProcessId           NVARCHAR(500)   NOT NULL,
    ProcessType         NVARCHAR(50)    NULL,
    ReplicaSetName      NVARCHAR(255)   NULL,
    ProjectKey          INT             NULL,
    OrgKey              INT             NULL,

    -- SKU
    InstanceSize        NVARCHAR(50)    NULL,
    ProviderName        NVARCHAR(50)    NULL,
    RegionName          NVARCHAR(100)   NULL,

    -- Time
    DateTimeEST         DATETIME        NULL,
    _date               DATE            NULL,
    _hour               INT             NULL,
    [type]              NVARCHAR(20)    NULL,
    businessHour        NVARCHAR(30)    NULL,

    -- CPU
    CpuAvg              FLOAT           NULL,
    CpuAvgP95           FLOAT           NULL,
    CpuMax              FLOAT           NULL,
    CpuMaxP95           FLOAT           NULL,
    CpuMaxGt50          INT             NULL,
    CpuMaxGt25          INT             NULL,
    CpuMaxGt10          INT             NULL,

    -- Memory RAW (MB)
    MemResidentMax      FLOAT           NULL,
    MemResidentAvg      FLOAT           NULL,
    MemAvailableMin     FLOAT           NULL,

    -- Memory % (calculated from MetaConfig)
    MemResidentMaxPct   FLOAT           NULL,
    MemResidentAvgPct   FLOAT           NULL,
    MemResidentP95Pct   FLOAT           NULL,

    -- Network
    NetInAvg            FLOAT           NULL,
    NetInMax            FLOAT           NULL,
    NetOutAvg           FLOAT           NULL,
    NetOutMax           FLOAT           NULL,
    NetRequestsMax      FLOAT           NULL,

    -- Connections
    ConnectionsMax      FLOAT           NULL,
    ConnectionsAvg      FLOAT           NULL,
    ConnUtilizationPct  FLOAT           NULL,

    -- Ops
    OpcQueryMax         FLOAT           NULL,
    OpcInsertMax        FLOAT           NULL,

    CONSTRAINT PK_MongoDBRightsizingAggregated5Min
        PRIMARY KEY CLUSTERED (Id ASC)
)
GO

-- Indexes
CREATE INDEX IX_MongoDB_5Min_Upsert
    ON [Metrics].[MongoDBRightsizingAggregated5Min]
    (ProcessId, DateTimeEST, _date, _hour, [type], businessHour)
GO

CREATE INDEX IX_MongoDB_5Min_Cluster
    ON [Metrics].[MongoDBRightsizingAggregated5Min]
    (ClusterKey, _date, ProcessType)
GO

-- Verify table created
SELECT
    ORDINAL_POSITION AS [#],
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'MongoDBRightsizingAggregated5Min'
ORDER BY ORDINAL_POSITION
GO


-- =============================================
-- SECTION 2: STORED PROCEDURE
-- usp_MongoDBRightsizingAggregatedMetrics5Min
-- Same logic as 15M proc
-- Only difference: all source tables use _5M
-- 12 readings per hour vs 4 in 15M
-- =============================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [Metrics].[usp_MongoDBRightsizingAggregatedMetrics5Min] AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartDT DATE = DATEADD(DAY, -7, CAST(GETDATE() AS DATE));
    DECLARE @EndDT   DATE = DATEADD(DAY, +1, CAST(GETDATE() AS DATE));

    -- Drop all temp tables
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
    -- 3. MEMORY RESIDENT + P95
    -- Source : MongoDB_Memory_Resident_5M
    -- P95 kept in temp table for % calculation
    -- Raw P95 NOT stored in final table
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
        HourBucket                  AS DateTimeEST,
        COALESCE(MemResidentMax, 0) AS MemResidentMax,
        COALESCE(MemResidentAvg, 0) AS MemResidentAvg,
        COALESCE(MemResidentP95, 0) AS MemResidentP95
    INTO #MemResident
    FROM Win WHERE rn = 1;

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
            MIN(Measurement) OVER (PARTITION BY [key], HourBucket)
                AS MemAvailableMin,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0)) AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                   AS DateTimeEST,
        COALESCE(MemAvailableMin, 0) AS MemAvailableMin
    INTO #MemAvail
    FROM Win WHERE rn = 1;

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
    -- Source : MongoDB_Connections_5M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Connections_5M]
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
    -- Source : MongoDB_Opcounter_Query_5M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Opcounter_Query_5M]
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
        HourBucket               AS DateTimeEST,
        COALESCE(OpcQueryMax, 0) AS OpcQueryMax
    INTO #OpcQuery
    FROM Win WHERE rn = 1;

    -- =========================================
    -- 12. OPCOUNTER INSERT
    -- Source : MongoDB_Opcounter_Insert_5M
    -- =========================================
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0)
                AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Opcounter_Insert_5M]
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
    -- MetaConfig provides:
    --   MemorySizeGB    → memory % calculation
    --   ConnectionLimit → connection % calculation
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

        -- SKU
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
        COALESCE(ma.MemAvailableMin, 0)                                     AS MemAvailableMin,

        -- Memory % from MetaConfig
        CASE WHEN COALESCE(m.MemorySizeGB, 0) > 0
            THEN ROUND((COALESCE(mr.MemResidentMax, 0)
                 / (m.MemorySizeGB * 1024)) * 100, 2)
            ELSE 0
        END                                                                 AS MemResidentMaxPct,

        CASE WHEN COALESCE(m.MemorySizeGB, 0) > 0
            THEN ROUND((COALESCE(mr.MemResidentAvg, 0)
                 / (m.MemorySizeGB * 1024)) * 100, 2)
            ELSE 0
        END                                                                 AS MemResidentAvgPct,

        CASE WHEN COALESCE(m.MemorySizeGB, 0) > 0
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

        -- Connection % from MetaConfig
        CASE WHEN COALESCE(m.ConnectionLimit, 0) > 0
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
    FROM [Metrics].[MongoDBRightsizingAggregated5Min] T
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
    INSERT INTO [Metrics].[MongoDBRightsizingAggregated5Min]
    (
        ClusterKey, ClusterName, ProcessId, ProcessType, ReplicaSetName,
        ProjectKey, OrgKey,
        InstanceSize, ProviderName, RegionName,
        DateTimeEST, _date, _hour, [type], businessHour,
        CpuAvg, CpuAvgP95, CpuMax, CpuMaxP95,
        CpuMaxGt50, CpuMaxGt25, CpuMaxGt10,
        MemResidentMax, MemResidentAvg, MemAvailableMin,
        MemResidentMaxPct, MemResidentAvgPct, MemResidentP95Pct,
        NetInAvg, NetInMax, NetOutAvg, NetOutMax, NetRequestsMax,
        ConnectionsMax, ConnectionsAvg, ConnUtilizationPct,
        OpcQueryMax, OpcInsertMax
    )
    SELECT
        S.ClusterKey, S.ClusterName, S.ProcessId, S.ProcessType, S.ReplicaSetName,
        S.ProjectKey, S.OrgKey,
        S.InstanceSize, S.ProviderName, S.RegionName,
        S.DateTimeEST, S._date, S._hour, S.[type], S.businessHour,
        S.CpuAvg, S.CpuAvgP95, S.CpuMax, S.CpuMaxP95,
        S.CpuMaxGt50, S.CpuMaxGt25, S.CpuMaxGt10,
        S.MemResidentMax, S.MemResidentAvg, S.MemAvailableMin,
        S.MemResidentMaxPct, S.MemResidentAvgPct, S.MemResidentP95Pct,
        S.NetInAvg, S.NetInMax, S.NetOutAvg, S.NetOutMax, S.NetRequestsMax,
        S.ConnectionsMax, S.ConnectionsAvg, S.ConnUtilizationPct,
        S.OpcQueryMax, S.OpcInsertMax
    FROM #FinalMetrics S
    WHERE NOT EXISTS (
        SELECT 1
        FROM [Metrics].[MongoDBRightsizingAggregated5Min] T
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
-- SECTION 3: EXECUTE + VERIFY
-- =============================================

EXEC [Metrics].[usp_MongoDBRightsizingAggregatedMetrics5Min]
GO

-- Row count check
SELECT
    COUNT(*)                        AS TotalRows,
    COUNT(DISTINCT ProcessId)       AS DistinctProcesses,
    COUNT(DISTINCT ClusterKey)      AS DistinctClusters,
    MIN(DateTimeEST)                AS MinDate,
    MAX(DateTimeEST)                AS MaxDate
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GO

-- Sample data — new columns
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
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ProcessType = 'REPLICA_PRIMARY'
ORDER BY DateTimeEST DESC
GO

-- Null check for new columns
SELECT
    COUNT(*)                                                    AS TotalRows,
    SUM(CASE WHEN CpuMaxP95          IS NULL THEN 1 ELSE 0 END) AS NullCpuMaxP95,
    SUM(CASE WHEN CpuAvgP95          IS NULL THEN 1 ELSE 0 END) AS NullCpuAvgP95,
    SUM(CASE WHEN MemResidentMaxPct  IS NULL THEN 1 ELSE 0 END) AS NullMemMaxPct,
    SUM(CASE WHEN MemResidentAvgPct  IS NULL THEN 1 ELSE 0 END) AS NullMemAvgPct,
    SUM(CASE WHEN MemResidentP95Pct  IS NULL THEN 1 ELSE 0 END) AS NullMemP95Pct,
    SUM(CASE WHEN ConnUtilizationPct IS NULL THEN 1 ELSE 0 END) AS NullConnPct
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GO