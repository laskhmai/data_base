-- STEP 1: Raw table date range
SELECT
    'Raw CPU Table'                     AS Source,
    MIN(CAST(DateTime AS DATE))         AS DataFrom,
    MAX(CAST(DateTime AS DATE))         AS DataTo,
    COUNT(DISTINCT CAST(DateTime AS DATE)) AS UniqueDays
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M]
WHERE FORMAT(DateTime,'yyyy-MM') = '2026-05'

UNION ALL

-- STEP 2: Aggregated table date range
SELECT
    'Aggregated Table'                  AS Source,
    MIN(_date)                          AS DataFrom,
    MAX(_date)                          AS DataTo,
    COUNT(DISTINCT _date)               AS UniqueDays
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE FORMAT(_date,'yyyy-MM') = '2026-05'
GO

-- STEP 3: Per day comparison
-- See if any days in raw but not in aggregated
SELECT
    r.RawDate,
    CASE WHEN a.AggDate IS NOT NULL
         THEN 'In Aggregated ✅'
         ELSE 'Missing from Aggregated ❌'
    END                                 AS AggregatedStatus
FROM (
    SELECT DISTINCT
        CAST(DateTime AS DATE)          AS RawDate
    FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M]
    WHERE FORMAT(DateTime,'yyyy-MM') = '2026-05'
) r
LEFT JOIN (
    SELECT DISTINCT
        _date                           AS AggDate
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
    WHERE FORMAT(_date,'yyyy-MM') = '2026-05'
) a ON a.AggDate = r.RawDate
ORDER BY r.RawDate
GO