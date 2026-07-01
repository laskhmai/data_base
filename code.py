SELECT
    ClusterName,
    _date,
    _hour,
    CpuAvg,
    CpuMax,
    CpuAvgP95,
    CpuMaxP95
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE ClusterName = 'cmsonc-eob-prod-cluster'
AND   CpuMax < 0
   OR CpuAvg < 0
GO

-- Also check count of negative rows overall
SELECT COUNT(*) AS NegativeRows
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE CpuMax < 0
OR    CpuAvg < 0
GO