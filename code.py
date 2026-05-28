-- Check ALL columns are populated correctly for cdr-uat
SELECT
    ClusterKey,
    ClusterName,
    _date,
    _hour,

    -- CPU checks
    CpuAvg,
    CpuAvgP95,         -- should be >= CpuAvg
    CpuMax,            -- should be >= CpuAvg
    CpuMaxP95,         -- should be between CpuAvg and CpuMax
    CpuMaxGt50,        -- count of 5min readings above 50%
    CpuMaxGt25,        -- count of 5min readings above 25%
    CpuMaxGt10,        -- count of 5min readings above 10%

    -- Memory RAW checks
    MemResidentMax,    -- MB — should be > 0
    MemResidentAvg,    -- MB — should be <= MemResidentMax
    MemAvailableMin,   -- KB — should be > 0

    -- Memory % checks
    MemResidentMaxPct, -- should be (MemResidentMax / RAM) × 100
    MemResidentAvgPct, -- should be <= MemResidentMaxPct
    MemResidentP95Pct, -- should be between Avg and Max

    -- Network checks
    NetInAvg,          -- should be > 0
    NetInMax,          -- should be >= NetInAvg
    NetOutAvg,         -- should be > 0
    NetOutMax,         -- should be >= NetOutAvg
    NetRequestsMax,    -- should be > 0

    -- Connection checks
    ConnectionsMax,    -- total all processes
    ConnectionsAvg,    -- should be <= ConnectionsMax
    ConnUtilizationPct,-- should be (ConnectionsMax/64000) × 100

    -- Ops checks
    OpcQueryMax,       -- should be > 0
    OpcInsertMax       -- should be > 0 or 0 if no inserts

FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterKey = 330
AND   _date      = '2026-05-21'
AND   _hour      = 8
GO


-- Summary — find any columns that are ALL zeros (not collecting)
SELECT
    -- CPU
    SUM(CASE WHEN CpuAvg        > 0 THEN 1 ELSE 0 END) AS CpuAvg_HasData,
    SUM(CASE WHEN CpuAvgP95     > 0 THEN 1 ELSE 0 END) AS CpuAvgP95_HasData,
    SUM(CASE WHEN CpuMax        > 0 THEN 1 ELSE 0 END) AS CpuMax_HasData,
    SUM(CASE WHEN CpuMaxP95     > 0 THEN 1 ELSE 0 END) AS CpuMaxP95_HasData,

    -- Memory RAW
    SUM(CASE WHEN MemResidentMax  > 0 THEN 1 ELSE 0 END) AS MemMax_HasData,
    SUM(CASE WHEN MemResidentAvg  > 0 THEN 1 ELSE 0 END) AS MemAvg_HasData,
    SUM(CASE WHEN MemAvailableMin > 0 THEN 1 ELSE 0 END) AS MemAvail_HasData,

    -- Memory %
    SUM(CASE WHEN MemResidentMaxPct > 0 THEN 1 ELSE 0 END) AS MemMaxPct_HasData,
    SUM(CASE WHEN MemResidentAvgPct > 0 THEN 1 ELSE 0 END) AS MemAvgPct_HasData,
    SUM(CASE WHEN MemResidentP95Pct > 0 THEN 1 ELSE 0 END) AS MemP95Pct_HasData,

    -- Network
    SUM(CASE WHEN NetInAvg       > 0 THEN 1 ELSE 0 END) AS NetInAvg_HasData,
    SUM(CASE WHEN NetInMax       > 0 THEN 1 ELSE 0 END) AS NetInMax_HasData,
    SUM(CASE WHEN NetOutAvg      > 0 THEN 1 ELSE 0 END) AS NetOutAvg_HasData,
    SUM(CASE WHEN NetOutMax      > 0 THEN 1 ELSE 0 END) AS NetOutMax_HasData,
    SUM(CASE WHEN NetRequestsMax > 0 THEN 1 ELSE 0 END) AS NetReq_HasData,

    -- Connections
    SUM(CASE WHEN ConnectionsMax    > 0 THEN 1 ELSE 0 END) AS ConnMax_HasData,
    SUM(CASE WHEN ConnUtilizationPct > 0 THEN 1 ELSE 0 END) AS ConnUtil_HasData,

    -- Ops
    SUM(CASE WHEN OpcQueryMax  > 0 THEN 1 ELSE 0 END) AS OpcQuery_HasData,
    SUM(CASE WHEN OpcInsertMax > 0 THEN 1 ELSE 0 END) AS OpcInsert_HasData,

    COUNT(*) AS TotalRows

FROM [Metrics].[MongoDBRightsizingAggregated5Min]
GO