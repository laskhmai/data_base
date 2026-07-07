SELECT
    r.ClusterName,
    r.DayType,
    r.HourType,
    r.Action,
    ROUND(MAX(a.CpuAvgP95), 2)      AS CpuAvgP95,
    ROUND(MAX(a.CpuMaxP95), 2)      AS CpuMaxP95,
    ROUND(MAX(a.CpuMax),    2)      AS PeakCpuMax,
    -- How often is CPU high?
    SUM(CASE WHEN a.CpuMax > 80
             THEN 1 ELSE 0 END)     AS HoursAbove80,
    SUM(CASE WHEN a.CpuMax > 50
             THEN 1 ELSE 0 END)     AS HoursAbove50,
    COUNT(*)                        AS TotalHours
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = r.ClusterKey
    AND a.[type]       = r.DayType
    AND a.businessHour = r.HourType
WHERE r.ClusterName IN (
    'cwx-cwih-patient-mdm-qa',
    'hqri-fhir-store-prd',
    'ma-dep-prod',
    'aiaa-app0002305-cld3-dev',
    'stars-gic-prod',
    'consumer-interops-prod'
)
AND r.Month  = '2026-06'
AND r.Action = 'Upsize'
GROUP BY
    r.ClusterName, r.DayType,
    r.HourType, r.Action
ORDER BY CpuAvgP95 ASC
GO