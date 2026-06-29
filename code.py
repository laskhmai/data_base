-- Check CpuMaxP95 values for cdr-uat
-- These are what level1 check uses (×2)
SELECT
    _date,
    _hour,
    [type],
    businessHour,
    ROUND(CpuAvg,    2)     AS CpuAvg,
    ROUND(CpuMax,    2)     AS CpuMax,
    ROUND(CpuAvgP95, 2)     AS CpuAvgP95,
    ROUND(CpuMaxP95, 2)     AS CpuMaxP95,
    -- What level1 check sees:
    ROUND(CpuMaxP95 * 2, 2) AS CpuMaxP95x2,
    ROUND(CpuAvgP95 * 2, 2) AS CpuAvgP95x2,
    -- Will it pass level1?
    CASE
        WHEN CpuMaxP95 * 2 < 100
        AND  CpuAvgP95 * 2 < 100
        THEN 'Pass → Downsize'
        ELSE 'Fail → NoChange'
    END                     AS Level1Check
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'cdr-uat'
AND   FORMAT(_date,'yyyy-MM') = '2026-05'
AND   [type]       = 'Weekday'
AND   businessHour = 'BusinessHours'
ORDER BY _date, _hour
GO

-- Summary for cdr-uat
SELECT
    ROUND(AVG(CpuMaxP95), 2)    AS AvgCpuMaxP95,
    ROUND(MAX(CpuMaxP95), 2)    AS MaxCpuMaxP95,
    ROUND(AVG(CpuAvgP95), 2)    AS AvgCpuAvgP95,
    ROUND(MAX(CpuAvgP95), 2)    AS MaxCpuAvgP95,
    -- Level1 check simulation
    ROUND(MAX(CpuMaxP95) * 2, 2) AS MaxCpuMaxP95x2,
    ROUND(MAX(CpuAvgP95) * 2, 2) AS MaxCpuAvgP95x2,
    COUNT(*) AS TotalHours
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'cdr-uat'
AND   FORMAT(_date,'yyyy-MM') = '2026-05'
AND   [type]       = 'Weekday'
AND   businessHour = 'BusinessHours'
GO