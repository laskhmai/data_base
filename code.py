SELECT
    r.ClusterName,
    r.DayType,
    r.HourType,
    r.Action,
    r.CpuRec,
    r.MemRec,
    r.ConnRec,
    r.Comment,
    ROUND(r.PeakCpuMax,    2) AS PeakCpuMax,
    ROUND(r.AvgCpuMax,     2) AS AvgCpuMax,
    ROUND(MAX(a.CpuMaxP95),2) AS CpuMaxP95,
    ROUND(MAX(a.CpuMaxP95)*2,2) AS CpuMaxP95x2,
    ROUND(MAX(a.ConnUtilizationPct),2) AS MaxConnUtil
FROM [Metrics].[MongoDBRightsizingRecommendations] r
JOIN [Metrics].[MongoDBRightsizingAggregated5Min] a
    ON  a.ClusterKey   = r.ClusterKey
    AND a.[type]       = r.DayType
    AND a.businessHour = r.HourType
WHERE r.ClusterName LIKE '%ins-cp-mgmt%'
AND   r.DayType  = 'Weekday'
AND   r.HourType = 'BusinessHours'
GROUP BY
    r.ClusterName, r.DayType, r.HourType,
    r.Action, r.CpuRec, r.MemRec, r.ConnRec,
    r.Comment, r.PeakCpuMax, r.AvgCpuMax
GO