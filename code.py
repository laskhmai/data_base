-- What does aggregated table show for cmsonc-eob-prod Weekend?
SELECT
    _date,
    _hour,
    [type],
    businessHour,
    ROUND(CpuMax,    2)     AS CpuMax,
    ROUND(CpuMaxP95, 2)     AS CpuMaxP95,
    ROUND(CpuMaxP95*2, 2)   AS CpuMaxP95x2
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'cmsonc-eob-prod-cluster'
AND   FORMAT(_date,'yyyy-MM') = '2026-06'
AND   [type]       = 'Weekend'
AND   businessHour = 'Weekend'
ORDER BY _date, _hour
GO