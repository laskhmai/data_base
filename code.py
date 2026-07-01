-- Clusters with high CpuMax in aggregated table
-- Check which clusters have dangerous CPU spikes
SELECT
    ClusterName,
    InstanceSize                            AS CurrentSku,
    FORMAT(_date,'yyyy-MM')                 AS Month,
    [type]                                  AS DayType,
    businessHour                            AS HourType,
    ROUND(AVG(CpuAvg),    2)               AS AvgCpuAvg,
    ROUND(AVG(CpuMax),    2)               AS AvgCpuMax,
    ROUND(MAX(CpuMax),    2)               AS PeakCpuMax,
    ROUND(AVG(CpuMaxP95), 2)               AS AvgCpuMaxP95,
    ROUND(MAX(CpuMaxP95), 2)               AS PeakCpuMaxP95,
    -- Level1 safety check
    ROUND(MAX(CpuMaxP95) * 2, 2)           AS CpuMaxP95x2,
    CASE
        WHEN MAX(CpuMaxP95) * 2 < 100
        THEN 'Safe to Downsize'
        ELSE 'High CPU — NoChange'
    END                                     AS SafetyStatus
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
GROUP BY
    ClusterName,
    InstanceSize,
    FORMAT(_date,'yyyy-MM'),
    [type],
    businessHour
HAVING MAX(CpuMax) > 60              -- high CpuMax threshold
ORDER BY PeakCpuMax DESC
GO

-- Summary: how many clusters are high CPU
SELECT
    CASE
        WHEN MAX(CpuMax) > 80
        THEN 'Critical (>80%)'
        WHEN MAX(CpuMax) > 60
        THEN 'High (60-80%)'
        ELSE 'Normal (<60%)'
    END                                     AS CpuCategory,
    COUNT(DISTINCT ClusterKey)              AS ClusterCount
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE FORMAT(_date,'yyyy-MM') = '2026-06'
GROUP BY
    CASE
        WHEN MAX(CpuMax) > 80
        THEN 'Critical (>80%)'
        WHEN MAX(CpuMax) > 60
        THEN 'High (60-80%)'
        ELSE 'Normal (<60%)'
    END
ORDER BY ClusterCount DESC
GO