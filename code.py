/*
===============================================================================
  DDL — Run ONCE to create the target table
===============================================================================
*/
CREATE TABLE [Metrics].[MongoDBRightsizingAggregatedHourly]
(
    -- Surrogate key
    Id                  BIGINT IDENTITY(1,1)    NOT NULL,

    -- Identity
    ClusterKey          INT                     NOT NULL,   -- FK to [MongoDB].[Clusters]
    ClusterName         NVARCHAR(255)           NULL,       -- e.g. coreapi-accums-qa
    ProcessId           NVARCHAR(255)           NOT NULL,   -- e.g. atlas-shard-00-00.mongodb.net:27017
    ProcessType         NVARCHAR(50)            NULL,       -- REPLICA_PRIMARY / REPLICA_SECONDARY
    ReplicaSetName      NVARCHAR(255)           NULL,       -- e.g. atlas-a2t3dq-shard-0
    ProjectKey          INT                     NULL,       -- FK to [MongoDB].[Projects]
    OrgKey              INT                     NULL,       -- FK to [MongoDB].[Organization]

    -- SKU / Infrastructure (parsed from ReplicationSpecs JSON in Clusters table)
    InstanceSize        NVARCHAR(20)            NULL,       -- e.g. M20, M30, M40
    ProviderName        NVARCHAR(50)            NULL,       -- e.g. AZURE
    RegionName          NVARCHAR(100)           NULL,       -- e.g. US_EAST_2

    -- Time dimensions
    DateTimeEST         DATETIME                NOT NULL,   -- hourly bucket in EST
    _date               DATE                    NOT NULL,
    _hour               INT                     NOT NULL,   -- 0-23
    [type]              NVARCHAR(10)            NOT NULL,   -- Weekday / Weekend
    businessHour        NVARCHAR(20)            NOT NULL,   -- BusinessHours / NonBusinessHours

    -- CPU (% normalized across cores — per process)
    CpuAvg              FLOAT                   NULL,       -- avg cpu % across the hour
    CpuMax              FLOAT                   NULL,       -- peak cpu % in the hour
    CpuMaxGt50          INT                     NULL,       -- readings where peak cpu > 50%
    CpuMaxGt25          INT                     NULL,       -- readings where peak cpu > 25%
    CpuMaxGt10          INT                     NULL,       -- readings where peak cpu > 10%

    -- Memory Resident (MEGABYTES — per process)
    MemResidentMax      FLOAT                   NULL,       -- peak MB used
    MemResidentAvg      FLOAT                   NULL,       -- avg MB used

    -- Memory Available (KILOBYTES — per process)
    MemAvailableMin     FLOAT                   NULL,       -- lowest free KB in the hour

    -- Connections (count — per process)
    ConnectionsMax      FLOAT                   NULL,       -- peak connections
    ConnectionsAvg      FLOAT                   NULL,       -- avg connections

    -- Opcounters (ops/sec — per process)
    OpcQueryMax         FLOAT                   NULL,       -- peak query ops/sec
    OpcInsertMax        FLOAT                   NULL,       -- peak insert ops/sec

    CONSTRAINT PK_MongoDBRightsizingAggregatedHourly
        PRIMARY KEY CLUSTERED (Id ASC)
);
GO

-- Index for upsert matching
CREATE NONCLUSTERED INDEX IX_MongoDB_Rightsizing_Upsert
    ON [Metrics].[MongoDBRightsizingAggregatedHourly]
    (ProcessId, DateTimeEST, _date, _hour, [type], businessHour);
GO

-- Index for reporting by cluster
CREATE NONCLUSTERED INDEX IX_MongoDB_Rightsizing_Cluster
    ON [Metrics].[MongoDBRightsizingAggregatedHourly]
    (ClusterKey, _date, ProcessType);
GO


/*
===============================================================================
  STORED PROCEDURE
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

    -- Default: last 7 days rolling
    DECLARE @StartDT DATE = DATEADD(DAY, -7, CAST(GETDATE() AS DATE));
    DECLARE @EndDT   DATE = DATEADD(DAY, +1, CAST(GETDATE() AS DATE));

    --------------------------------------------------------------------------
    -- Drop temp tables if they exist
    --------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#CpuAvg')      IS NOT NULL DROP TABLE #CpuAvg;
    IF OBJECT_ID('tempdb..#CpuMax')      IS NOT NULL DROP TABLE #CpuMax;
    IF OBJECT_ID('tempdb..#MemResident') IS NOT NULL DROP TABLE #MemResident;
    IF OBJECT_ID('tempdb..#MemAvail')    IS NOT NULL DROP TABLE #MemAvail;
    IF OBJECT_ID('tempdb..#Conns')       IS NOT NULL DROP TABLE #Conns;
    IF OBJECT_ID('tempdb..#OpcQuery')    IS NOT NULL DROP TABLE #OpcQuery;
    IF OBJECT_ID('tempdb..#OpcInsert')   IS NOT NULL DROP TABLE #OpcInsert;
    IF OBJECT_ID('tempdb..#Keys')        IS NOT NULL DROP TABLE #Keys;
    IF OBJECT_ID('tempdb..#FinalMetrics')IS NOT NULL DROP TABLE #FinalMetrics;

    --------------------------------------------------------------------------
    -- HELPER: Convert DateTime to EST hour bucket
    -- All metrics tables have a DateTime column in UTC
    -- SWITCHOFFSET converts to EST (-05:00)
    -- DATEADD/DATEDIFF truncates to the hour
    --------------------------------------------------------------------------

    --------------------------------------------------------------------------
    -- 1. CPU AVG
    --    Source : MongoDB_System_Normalized_Cpu_User_15M
    --    Unit   : Percent (normalized across cores)
    --    Per    : Process per hour
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_15M]
        WHERE DateTime >= @StartDT
          AND DateTime <  @EndDT
    ),
    Win AS (
        SELECT
            [key],
            HourBucket,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket) AS CpuAvg,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket              AS DateTimeEST,
        COALESCE(CpuAvg, 0)    AS CpuAvg
    INTO #CpuAvg
    FROM Win
    WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 2. CPU MAX
    --    Source : MongoDB_System_Normalized_Cpu_User_Max_15M
    --    Unit   : Percent (normalized across cores)
    --    Per    : Process per hour
    --    Note   : Thresholds use 50/25/10 NOT 88/50/25
    --             because normalized CPU rarely exceeds 88%
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_15M]
        WHERE DateTime >= @StartDT
          AND DateTime <  @EndDT
    ),
    Win AS (
        SELECT
            [key],
            HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS CpuMax,
            -- Gt50: high pressure
            SUM(CASE WHEN Measurement > 50 THEN 1 ELSE 0 END)
                OVER (PARTITION BY [key], HourBucket)              AS CpuMaxGt50,
            -- Gt25: medium pressure
            SUM(CASE WHEN Measurement > 25 THEN 1 ELSE 0 END)
                OVER (PARTITION BY [key], HourBucket)              AS CpuMaxGt25,
            -- Gt10: light pressure / not idle
            SUM(CASE WHEN Measurement > 10 THEN 1 ELSE 0 END)
                OVER (PARTITION BY [key], HourBucket)              AS CpuMaxGt10,
            ROW_NUMBER() OVER (PARTITION BY [key], HourBucket
                               ORDER BY (SELECT 0))                AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                  AS DateTimeEST,
        COALESCE(CpuMax, 0)        AS CpuMax,
        COALESCE(CpuMaxGt50, 0)    AS CpuMaxGt50,
        COALESCE(CpuMaxGt25, 0)    AS CpuMaxGt25,
        COALESCE(CpuMaxGt10, 0)    AS CpuMaxGt10
    INTO #CpuMax
    FROM Win
    WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 3. MEMORY RESIDENT
    --    Source : MongoDB_Memory_Resident_15M
    --    Unit   : MEGABYTES
    --    Per    : Process per hour
    --    Note   : No % thresholds — compare raw MB vs tier RAM in reporting
    --             M20=8GB, M30=16GB, M40=32GB, M50=64GB
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Memory_Resident_15M]
        WHERE DateTime >= @StartDT
          AND DateTime <  @EndDT
    ),
    Win AS (
        SELECT
            [key],
            HourBucket,
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
    FROM Win
    WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 4. MEMORY AVAILABLE
    --    Source : MongoDB_System_Memory_Available_15M
    --    Unit   : KILOBYTES
    --    Per    : Process per hour
    --    Note   : MIN = worst case free memory in the hour
    --             If near zero = process is memory starved
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_System_Memory_Available_15M]
        WHERE DateTime >= @StartDT
          AND DateTime <  @EndDT
    ),
    Win AS (
        SELECT
            [key],
            HourBucket,
            MIN(Measurement) OVER (PARTITION BY [key], HourBucket) AS MemAvailableMin,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                      AS DateTimeEST,
        COALESCE(MemAvailableMin, 0)   AS MemAvailableMin
    INTO #MemAvail
    FROM Win
    WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 5. CONNECTIONS
    --    Source : MongoDB_Connections_15M
    --    Unit   : Scalar (count)
    --    Per    : Process per hour
    --    Note   : Each tier has a hard connection limit
    --             M20=3000, M30=6000, M40=16000
    --             Report layer compares max vs tier limit
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Connections_15M]
        WHERE DateTime >= @StartDT
          AND DateTime <  @EndDT
    ),
    Win AS (
        SELECT
            [key],
            HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS ConnectionsMax,
            AVG(Measurement) OVER (PARTITION BY [key], HourBucket) AS ConnectionsAvg,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                      AS DateTimeEST,
        COALESCE(ConnectionsMax, 0)    AS ConnectionsMax,
        COALESCE(ConnectionsAvg, 0)    AS ConnectionsAvg
    INTO #Conns
    FROM Win
    WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 6. OPCOUNTER QUERY
    --    Source : MongoDB_Opcounter_Query_15M
    --    Unit   : Scalar_P (ops/sec)
    --    Per    : Process per hour
    --    Note   : Queries can hit PRIMARY or SECONDARY
    --             MAX tells you peak read pressure
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Opcounter_Query_15M]
        WHERE DateTime >= @StartDT
          AND DateTime <  @EndDT
    ),
    Win AS (
        SELECT
            [key],
            HourBucket,
            MAX(Measurement) OVER (PARTITION BY [key], HourBucket) AS OpcQueryMax,
            ROW_NUMBER()     OVER (PARTITION BY [key], HourBucket
                                   ORDER BY (SELECT 0))             AS rn
        FROM Raw
    )
    SELECT
        [key],
        HourBucket                  AS DateTimeEST,
        COALESCE(OpcQueryMax, 0)   AS OpcQueryMax
    INTO #OpcQuery
    FROM Win
    WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 7. OPCOUNTER INSERT
    --    Source : MongoDB_Opcounter_Insert_15M
    --    Unit   : Scalar_P (ops/sec)
    --    Per    : Process per hour
    --    Note   : Inserts ONLY go to PRIMARY
    --             High insert rate on PRIMARY = write pressure = scale up signal
    --------------------------------------------------------------------------
    ;WITH Raw AS (
        SELECT
            [key],
            DATEADD(HOUR, DATEDIFF(HOUR, 0,
                SWITCHOFFSET(CONVERT(datetimeoffset, DateTime), '-05:00')), 0) AS HourBucket,
            Measurement
        FROM [Metrics].[MongoDB_Opcounter_Insert_15M]
        WHERE DateTime >= @StartDT
          AND DateTime <  @EndDT
    ),
    Win AS (
        SELECT
            [key],
            HourBucket,
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
    FROM Win
    WHERE rn = 1;

    --------------------------------------------------------------------------
    -- 8. Keys spine
    --    Union ALL process+hour combos that appeared in ANY metric table
    --    This ensures no hour is lost even if one metric had no data
    --------------------------------------------------------------------------
    SELECT [key], DateTimeEST INTO #Keys FROM #CpuAvg
    UNION SELECT [key], DateTimeEST FROM #CpuMax
    UNION SELECT [key], DateTimeEST FROM #MemResident
    UNION SELECT [key], DateTimeEST FROM #MemAvail
    UNION SELECT [key], DateTimeEST FROM #Conns
    UNION SELECT [key], DateTimeEST FROM #OpcQuery
    UNION SELECT [key], DateTimeEST FROM #OpcInsert;

    --------------------------------------------------------------------------
    -- 9. Final assembly
    --    Join chain:
    --      #Keys.[key]
    --        = [MongoDB].[Process].ProcessId     (get process context)
    --        → [MongoDB].[Process].ClusterKey
    --          = [MongoDB].[Clusters].ClustersKey (get SKU from JSON)
    --
    --    No GROUP BY — stays at process level
    --    One row per process per hour
    --
    --    InstanceSize parsed from ReplicationSpecs JSON:
    --    JSON path may need adjusting based on your exact JSON structure
    --    Run: SELECT TOP 1 ReplicationSpecs FROM [MongoDB].[Clusters]
    --    to verify the path
    --------------------------------------------------------------------------
    SELECT
        -- Identity
        p.ClusterKey,
        cl.Name                                                         AS ClusterName,
        k.[key]                                                         AS ProcessId,
        p.ProcessType,
        p.ReplicaSetName,
        p.ProjectKey,
        p.OrgKey,

        -- SKU from Clusters.ReplicationSpecs JSON
        JSON_VALUE(cl.ReplicationSpecs,
            '$[0].regionConfigs[0].electableSpecs.instanceSize')        AS InstanceSize,
        JSON_VALUE(cl.ReplicationSpecs,
            '$[0].regionConfigs[0].providerName')                       AS ProviderName,
        JSON_VALUE(cl.ReplicationSpecs,
            '$[0].regionConfigs[0].regionName')                         AS RegionName,

        -- Time
        k.DateTimeEST,
        CAST(k.DateTimeEST AS DATE)                                     AS _date,
        DATEPART(HOUR, k.DateTimeEST)                                   AS _hour,
        CASE WHEN DATEPART(WEEKDAY, k.DateTimeEST) IN (1, 7)
             THEN 'Weekend' ELSE 'Weekday' END                          AS [type],
        CASE WHEN DATEPART(HOUR, k.DateTimeEST) BETWEEN 7 AND 18
             THEN 'BusinessHours' ELSE 'NonBusinessHours' END           AS businessHour,

        -- CPU
        COALESCE(ca.CpuAvg, 0)      AS CpuAvg,
        COALESCE(cm.CpuMax, 0)      AS CpuMax,
        COALESCE(cm.CpuMaxGt50, 0)  AS CpuMaxGt50,
        COALESCE(cm.CpuMaxGt25, 0)  AS CpuMaxGt25,
        COALESCE(cm.CpuMaxGt10, 0)  AS CpuMaxGt10,

        -- Memory Resident (MB)
        COALESCE(mr.MemResidentMax, 0)  AS MemResidentMax,
        COALESCE(mr.MemResidentAvg, 0)  AS MemResidentAvg,

        -- Memory Available (KB)
        COALESCE(ma.MemAvailableMin, 0) AS MemAvailableMin,

        -- Connections
        COALESCE(cn.ConnectionsMax, 0)  AS ConnectionsMax,
        COALESCE(cn.ConnectionsAvg, 0)  AS ConnectionsAvg,

        -- Opcounters (ops/sec)
        COALESCE(oq.OpcQueryMax, 0)     AS OpcQueryMax,
        COALESCE(oi.OpcInsertMax, 0)    AS OpcInsertMax

    INTO #FinalMetrics
    FROM #Keys k
    -- Process table: metrics [key] = ProcessId
    JOIN [MongoDB].[Process] p
        ON  p.ProcessId  = k.[key]
        AND p.IsDeleted  = 0
    -- Clusters table: get SKU info from JSON
    JOIN [MongoDB].[Clusters] cl
        ON  cl.ClustersKey = p.ClusterKey
    -- Metric temp tables (all LEFT JOIN — metric may not exist for every hour)
    LEFT JOIN #CpuAvg      ca  ON ca.[key]  = k.[key] AND ca.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #CpuMax      cm  ON cm.[key]  = k.[key] AND cm.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #MemResident mr  ON mr.[key]  = k.[key] AND mr.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #MemAvail    ma  ON ma.[key]  = k.[key] AND ma.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #Conns       cn  ON cn.[key]  = k.[key] AND cn.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #OpcQuery    oq  ON oq.[key]  = k.[key] AND oq.DateTimeEST  = k.DateTimeEST
    LEFT JOIN #OpcInsert   oi  ON oi.[key]  = k.[key] AND oi.DateTimeEST  = k.DateTimeEST;

    --------------------------------------------------------------------------
    -- 10. UPSERT into [Metrics].[MongoDBRightsizingAggregatedHourly]
    --     Match key: ProcessId + DateTimeEST + _date + _hour + type + businessHour
    --     Same pattern as PostgreSQL proc
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
        T.CpuAvg            = S.CpuAvg,
        T.CpuMax            = S.CpuMax,
        T.CpuMaxGt50        = S.CpuMaxGt50,
        T.CpuMaxGt25        = S.CpuMaxGt25,
        T.CpuMaxGt10        = S.CpuMaxGt10,
        T.MemResidentMax    = S.MemResidentMax,
        T.MemResidentAvg    = S.MemResidentAvg,
        T.MemAvailableMin   = S.MemAvailableMin,
        T.ConnectionsMax    = S.ConnectionsMax,
        T.ConnectionsAvg    = S.ConnectionsAvg,
        T.OpcQueryMax       = S.OpcQueryMax,
        T.OpcInsertMax      = S.OpcInsertMax
    FROM [Metrics].[MongoDBRightsizingAggregatedHourly] T
    JOIN #FinalMetrics S
        ON  T.ProcessId     = S.ProcessId
        AND T.DateTimeEST   = S.DateTimeEST
        AND T._date         = S._date
        AND T._hour         = S._hour
        AND T.[type]        = S.[type]
        AND T.businessHour  = S.businessHour;

    -- INSERT new rows
    INSERT INTO [Metrics].[MongoDBRightsizingAggregatedHourly]
    (
        ClusterKey, ClusterName, ProcessId, ProcessType, ReplicaSetName,
        ProjectKey, OrgKey,
        InstanceSize, ProviderName, RegionName,
        DateTimeEST, _date, _hour, [type], businessHour,
        CpuAvg, CpuMax, CpuMaxGt50, CpuMaxGt25, CpuMaxGt10,
        MemResidentMax, MemResidentAvg, MemAvailableMin,
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
