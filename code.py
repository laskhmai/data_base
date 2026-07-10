SELECT
    _date,
    [type],
    businessHour,
    ROUND(CpuMaxP95, 2) AS CpuMaxP95,
    ROUND(CpuMax,    2) AS CpuMax
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'cc-atlas-dev-1'
AND   ClusterKey  = 131
AND   FORMAT(_date,'yyyy-MM') = '2026-06'
AND   [type] = 'Weekend'
ORDER BY _date
GO