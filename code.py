/*
===============================================================================
  STEP 1 — DROP existing table (since we are still in testing)
===============================================================================
*/
IF OBJECT_ID('[Metrics].[MongoDBRightsizingAggregatedHourly]') IS NOT NULL
    DROP TABLE [Metrics].[MongoDBRightsizingAggregatedHourly];
GO

/*
===============================================================================
  STEP 2 — CREATE TABLE (corrected — all 32 columns)
===============================================================================
*/
CREATE TABLE [Metrics].[MongoDBRightsizingAggregatedHourly]
(
    -- Surrogate key
    Id                  BIGINT IDENTITY(1,1)    NOT NULL,

    -- Identity
    ClusterKey          INT                     NOT NULL,
    ClusterName         NVARCHAR(255)           NULL,
    ProcessId           NVARCHAR(255)           NOT NULL,
    ProcessType         NVARCHAR(50)            NULL,
    ReplicaSetName      NVARCHAR(255)           NULL,
    ProjectKey          INT                     NULL,
    OrgKey              INT                     NULL,

    -- SKU (from ReplicationSpecs JSON)
    InstanceSize        NVARCHAR(20)            NULL,
    ProviderName        NVARCHAR(50)            NULL,
    RegionName          NVARCHAR(100)           NULL,

    -- Time
    DateTimeEST         DATETIME                NOT NULL,
    _date               DATE                    NOT NULL,
    _hour               INT                     NOT NULL,
    [type]              NVARCHAR(10)            NOT NULL,
    businessHour        NVARCHAR(20)            NOT NULL,

    -- CPU % normalized (per process)
    CpuAvg              FLOAT                   NULL,
    CpuMax              FLOAT                   NULL,
    CpuMaxGt50          INT                     NULL,
    CpuMaxGt25          INT                     NULL,
    CpuMaxGt10          INT                     NULL,

    -- Memory Resident MB (per process) — source: _5M (15M is DISABLED)
    MemResidentMax      FLOAT                   NULL,
    MemResidentAvg      FLOAT                   NULL,

    -- Memory Available KB (per process)
    MemAvailableMin     FLOAT                   NULL,

    -- Network BytesPerSec (per process)
    NetInAvg            FLOAT                   NULL,
    NetInMax            FLOAT                   NULL,
    NetOutAvg           FLOAT                   NULL,
    NetOutMax           FLOAT                   NULL,
    NetRequestsMax      FLOAT                   NULL,

    -- Connections (per process)
    ConnectionsMax      FLOAT                   NULL,
    ConnectionsAvg      FLOAT                   NULL,

    -- Opcounters ops/sec (per process)
    OpcQueryMax         FLOAT                   NULL,
    OpcInsertMax        FLOAT                   NULL,

    CONSTRAINT PK_MongoDBRightsizingAggregatedHourly
        PRIMARY KEY CLUSTERED (Id ASC)
);
GO

CREATE NONCLUSTERED INDEX IX_MongoDB_Rightsizing_Upsert
    ON [Metrics].[MongoDBRightsizingAggregatedHourly]
    (ProcessId, DateTimeEST, _date, _hour, [type], businessHour);
GO

CREATE NONCLUSTERED INDEX IX_MongoDB_Rightsizing_Cluster
    ON [Metrics].[MongoDBRightsizingAggregatedHourly]
    (ClusterKey, _date, ProcessType);
GO


/*
===============================================================================
  STEP 3 — STORED PROCEDURE (corrected — all 12 metrics + Memory fix)
===============================================================================
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [Metrics].[usp_MongoDBRightsizingAggregatedMetrics] AS
BEGIN
    SET NOCOUNT ON;

    --DECLARE @StartDate DATE = '2026-04-20';
    --DECLARE @EndDate   DATE = '2026-04-28';

    DECLARE @StartDT DATE = DATEADD(DAY, -7, CAST(GETDATE() AS DATE));
    DECLARE @EndDT   DATE = DATEADD(DAY, +1, CAST(GETDATE() AS DATE));

    --------------------------------------------------------------------------
    -- Drop all temp tables
    --------------------------------------------------------------------------
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

    --------------------------------------------------------------------------
    -- 1. CPU AVG
    --    Source : MongoDB_System_Normalized_Cpu_User_15M
    --    Unit   : Percent normalized across cores
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket) AS CpuAvg,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket           AS DateTimeEST,
        COALESCE(CpuAvg, 0) AS CpuAvg
    INTO #CpuAvg
    FROM Win WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 2. CPU MAX
    --    Source : MongoDB_System_Normalized_Cpu_User_Max_15M
    --    Unit   : Percent normalized across cores
    --    Note   : Thresholds 50/25/10 (NOT 88) — normalized CPU rarely hits 88
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS CpuMax,
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
    SELECT
        [key],
        HourBucket                AS DateTimeEST,
        COALESCE(CpuMax, 0)      AS CpuMax,
        COALESCE(CpuMaxGt50, 0) AS CpuMaxGt50,
        COALESCE(CpuMaxGt25, 0) AS CpuMaxGt25,
        COALESCE(CpuMaxGt10, 0) AS CpuMaxGt10
    INTO #CpuMax
    FROM Win WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 3. MEMORY RESIDENT
    --    Source : MongoDB_Memory_Resident_5M
    --    *** NOTE: Using 5M because 15M is DISABLED (Enabled=0 in MongoDbSettings)
    --    Unit   : MEGABYTES per process
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Memory_Resident_5M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS MemResidentMax,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket) AS MemResidentAvg,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                      AS DateTimeEST,
        COALESCE(MemResidentMax, 0)    AS MemResidentMax,
        COALESCE(MemResidentAvg, 0)    AS MemResidentAvg
    INTO #MemResident
    FROM Win WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 4. MEMORY AVAILABLE
    --    Source : MongoDB_System_Memory_Available_15M
    --    Unit   : KILOBYTES — MIN = worst case free memory in the hour
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Memory_Available_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MIN(Measurement) OVER (PARTITION BY [key], HourBucket) AS MemAvailableMin,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                     AS DateTimeEST,
        COALESCE(MemAvailableMin, 0)  AS MemAvailableMin
    INTO #MemAvail
    FROM Win WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 5. NETWORK IN AVG
    --    Source : MongoDB_System_Network_In_15M
    --    Unit   : BytesPerSec
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Network_In_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket) AS NetInAvg,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket              AS DateTimeEST,
        COALESCE(NetInAvg, 0)  AS NetInAvg
    INTO #NetIn
    FROM Win WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 6. NETWORK IN MAX
    --    Source : MongoDB_System_Network_In_Max_15M
    --    Unit   : BytesPerSec
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Network_In_Max_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS NetInMax,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket              AS DateTimeEST,
        COALESCE(NetInMax, 0)  AS NetInMax
    INTO #NetInMax
    FROM Win WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 7. NETWORK OUT AVG
    --    Source : MongoDB_System_Network_Out_15M
    --    Unit   : BytesPerSec
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Network_Out_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket) AS NetOutAvg,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket               AS DateTimeEST,
        COALESCE(NetOutAvg, 0)  AS NetOutAvg
    INTO #NetOut
    FROM Win WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 8. NETWORK OUT MAX
    --    Source : MongoDB_System_Network_Out_Max_15M
    --    Unit   : BytesPerSec
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Network_Out_Max_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS NetOutMax,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket               AS DateTimeEST,
        COALESCE(NetOutMax, 0)  AS NetOutMax
    INTO #NetOutMax
    FROM Win WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 9. NETWORK NUM REQUESTS
    --    Source : MongoDB_Network_Num_Requests_15M
    --    Unit   : Scalar_P (requests per second)
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Network_Num_Requests_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS NetRequestsMax,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                    AS DateTimeEST,
        COALESCE(NetRequestsMax, 0)  AS NetRequestsMax
    INTO #NetRequests
    FROM Win WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 10. CONNECTIONS
    --     Source : MongoDB_Connections_15M
    --     Unit   : Scalar count per process
    --     Note   : M10=1500, M20=3000, M30=6000, M40=16000 hard limits
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Connections_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS ConnectionsMax,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket) AS ConnectionsAvg,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                     AS DateTimeEST,
        COALESCE(ConnectionsMax, 0)   AS ConnectionsMax,
        COALESCE(ConnectionsAvg, 0)   AS ConnectionsAvg
    INTO #Conns
    FROM Win WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 11. OPCOUNTER QUERY
    --     Source : MongoDB_Opcounter_Query_15M
    --     Unit   : Scalar_P ops/sec
    --     Note   : Can hit PRIMARY or SECONDARY
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Opcounter_Query_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS OpcQueryMax,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                 AS DateTimeEST,
        COALESCE(OpcQueryMax, 0)  AS OpcQueryMax
    INTO #OpcQuery
    FROM Win WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 12. OPCOUNTER INSERT
    --     Source : MongoDB_Opcounter_Insert_15M
    --     Unit   : Scalar_P ops/sec
    --     Note   : Inserts ONLY go to PRIMARY — high value = write pressure
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Opcounter_Insert_15M]
        WHERE DateTime >= @StartDT AND DateTime < @EndDT
    ),
    Win AS (
        SELECT
            [key], HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS OpcInsertMax,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                  AS DateTimeEST,
        COALESCE(OpcInsertMax, 0)  AS OpcInsertMax
    INTO #OpcInsert
    FROM Win WHERE rn = 1;

    --------------------------------------------------------------------------
    -- Keys spine — union ALL process+hour combos across ALL 12 metric tables
    --------------------------------------------------------------------------
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

    --------------------------------------------------------------------------
    -- Final assembly
    -- Join chain:
    --   #Keys.[key]
    --     = [MongoDB].[Process].ProcessId    → get process context
    --     → [MongoDB].[Process].ClusterKey
    --       = [MongoDB].[Clusters].ClustersKey → get SKU from JSON
    --
    -- One row per PROCESS per HOUR — no aggregation
    -- Keeps PRIMARY vs SECONDARY split for reporting
    --------------------------------------------------------------------------
    SELECT
        -- Identity
        p.ClusterKey,
        cl.Name                                                          AS ClusterName,
        k.[key]                                                          AS ProcessId,
        p.ProcessType,
        p.ReplicaSetName,
        p.ProjectKey,
        p.OrgKey,

        -- SKU from Clusters.ReplicationSpecs JSON
        JSON_VALUE(cl.ReplicationSpecs,
            '$[0].regionConfigs[0].effectiveElectableSpecs.instanceSize') AS InstanceSize,
        JSON_VALUE(cl.ReplicationSpecs,
            '$[0].regionConfigs[0].providerName')                         AS ProviderName,
        JSON_VALUE(cl.ReplicationSpecs,
            '$[0].regionConfigs[0].regionName')                           AS RegionName,

        -- Time
        k.DateTimeEST,
        CAST(k.DateTimeEST AS DATE)                                       AS _date,
        DATEPART(HOUR, k.DateTimeEST)                                     AS _hour,
        CASE WHEN DATEPART(WEEKDAY, k.DateTimeEST) IN (1, 7)
             THEN 'Weekend' ELSE 'Weekday' END                            AS [type],
        CASE WHEN DATEPART(HOUR, k.DateTimeEST) BETWEEN 7 AND 18
             THEN 'BusinessHours' ELSE 'NonBusinessHours' END             AS businessHour,

        -- CPU
        COALESCE(ca.CpuAvg, 0)      AS CpuAvg,
        COALESCE(cm.CpuMax, 0)      AS CpuMax,
        COALESCE(cm.CpuMaxGt50, 0)  AS CpuMaxGt50,
        COALESCE(cm.CpuMaxGt25, 0)  AS CpuMaxGt25,
        COALESCE(cm.CpuMaxGt10, 0)  AS CpuMaxGt10,

        -- Memory Resident MB (from _5M — 15M is disabled)
        COALESCE(mr.MemResidentMax, 0)  AS MemResidentMax,
        COALESCE(mr.MemResidentAvg, 0)  AS MemResidentAvg,

        -- Memory Available KB
        COALESCE(ma.MemAvailableMin, 0) AS MemAvailableMin,

        -- Network BytesPerSec
        COALESCE(ni.NetInAvg, 0)        AS NetInAvg,
        COALESCE(nim.NetInMax, 0)       AS NetInMax,
        COALESCE(no2.NetOutAvg, 0)      AS NetOutAvg,
        COALESCE(nom.NetOutMax, 0)      AS NetOutMax,
        COALESCE(nr.NetRequestsMax, 0)  AS NetRequestsMax,

        -- Connections
        COALESCE(cn.ConnectionsMax, 0)  AS ConnectionsMax,
        COALESCE(cn.ConnectionsAvg, 0)  AS ConnectionsAvg,

        -- Opcounters ops/sec
        COALESCE(oq.OpcQueryMax, 0)     AS OpcQueryMax,
        COALESCE(oi.OpcInsertMax, 0)    AS OpcInsertMax

    INTO #FinalMetrics
    FROM #Keys k
    JOIN [MongoDB].[Process] p
        ON  p.ProcessId = k.[key]
        AND p.IsDeleted = 0
    JOIN [MongoDB].[Clusters] cl
        ON  cl.ClustersKey = p.ClusterKey
    LEFT JOIN #CpuAvg      ca   ON ca.[key]  = k.[key] AND ca.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #CpuMax      cm   ON cm.[key]  = k.[key] AND cm.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #MemResident mr   ON mr.[key]  = k.[key] AND mr.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #MemAvail    ma   ON ma.[key]  = k.[key] AND ma.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #NetIn       ni   ON ni.[key]  = k.[key] AND ni.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #NetInMax    nim  ON nim.[key] = k.[key] AND nim.DateTimeEST = k.DateTimeEST
    LEFT JOIN #NetOut      no2  ON no2.[key] = k.[key] AND no2.DateTimeEST = k.DateTimeEST
    LEFT JOIN #NetOutMax   nom  ON nom.[key] = k.[key] AND nom.DateTimeEST = k.DateTimeEST
    LEFT JOIN #NetRequests nr   ON nr.[key]  = k.[key] AND nr.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #Conns       cn   ON cn.[key]  = k.[key] AND cn.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #OpcQuery    oq   ON oq.[key]  = k.[key] AND oq.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #OpcInsert   oi   ON oi.[key]  = k.[key] AND oi.DateTimeEST  = k.DateTimeEST;

    --------------------------------------------------------------------------
    -- UPSERT into [Metrics].[MongoDBRightsizingAggregatedHourly]
    -- Match key: ProcessId + DateTimeEST + _date + _hour + type + businessHour
    --------------------------------------------------------------------------

    -- UPDATE existing rows
    UPDATE T
    SET
        T.ClusterKey        = S.ClusterKey,
        T.ClusterName       = S.ClusterName,
        T.ProcessType       = S.ProcessType,
        T.ReplicaSetName    = S.ReplicaSetName,
        T.ProjectKey        = S.ProjectKey,
        T.OrgKey            = S.OrgKey,
        T.InstanceSize      = S.InstanceSize,
        T.ProviderName      = S.ProviderName,
        T.RegionName        = S.RegionName,
        -- CPU
        T.CpuAvg            = S.CpuAvg,
        T.CpuMax            = S.CpuMax,
        T.CpuMaxGt50        = S.CpuMaxGt50,
        T.CpuMaxGt25        = S.CpuMaxGt25,
        T.CpuMaxGt10        = S.CpuMaxGt10,
        -- Memory
        T.MemResidentMax    = S.MemResidentMax,
        T.MemResidentAvg    = S.MemResidentAvg,
        T.MemAvailableMin   = S.MemAvailableMin,
        -- Network
        T.NetInAvg          = S.NetInAvg,
        T.NetInMax          = S.NetInMax,
        T.NetOutAvg         = S.NetOutAvg,
        T.NetOutMax         = S.NetOutMax,
        T.NetRequestsMax    = S.NetRequestsMax,
        -- Connections
        T.ConnectionsMax    = S.ConnectionsMax,
        T.ConnectionsAvg    = S.ConnectionsAvg,
        -- Opcounters
        T.OpcQueryMax       = S.OpcQueryMax,
        T.OpcInsertMax      = S.OpcInsertMax
    FROM [Metrics].[MongoDBRightsizingAggregatedHourly] T
    JOIN #FinalMetrics S
        ON  T.ProcessId    = S.ProcessId
        AND T.DateTimeEST  = S.DateTimeEST
        AND T._date        = S._date
        AND T._hour        = S._hour
        AND T.[type]       = S.[type]
        AND T.businessHour = S.businessHour;

    -- INSERT new rows
    INSERT INTO [Metrics].[MongoDBRightsizingAggregatedHourly]
    (
        ClusterKey, ClusterName, ProcessId, ProcessType, ReplicaSetName,
        ProjectKey, OrgKey,
        InstanceSize, ProviderName, RegionName,
        DateTimeEST, _date, _hour, [type], businessHour,
        CpuAvg, CpuMax, CpuMaxGt50, CpuMaxGt25, CpuMaxGt10,
        MemResidentMax, MemResidentAvg, MemAvailableMin,
        NetInAvg, NetInMax, NetOutAvg, NetOutMax, NetRequestsMax,
        ConnectionsMax, ConnectionsAvg,
        OpcQueryMax, OpcInsertMax
    )
    SELECT
        S.ClusterKey, S.ClusterName, S.ProcessId, S.ProcessType, S.ReplicaSetName,
        S.ProjectKey, S.OrgKey,
        S.InstanceSize, S.ProviderName, S.RegionName,
        S.DateTimeEST, S._date, S._hour, S.[type], S.businessHour,
        S.CpuAvg, S.CpuMax, S.CpuMaxGt50, S.CpuMaxGt25, S.CpuMaxGt10,
        S.MemResidentMax, S.MemResidentAvg, S.MemAvailableMin,
        S.NetInAvg, S.NetInMax, S.NetOutAvg, S.NetOutMax, S.NetRequestsMax,
        S.ConnectionsMax, S.ConnectionsAvg,
        S.OpcQueryMax, S.OpcInsertMax
    FROM #FinalMetrics S
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM [Metrics].[MongoDBRightsizingAggregatedHourly] T
        WHERE T.ProcessId    = S.ProcessId
          AND T.DateTimeEST  = S.DateTimeEST
          AND T._date        = S._date
          AND T._hour        = S._hour
          AND T.[type]       = S.[type]
          AND T.businessHour = S.businessHour
    );

END
GO
