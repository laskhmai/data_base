-- Get 3 interesting clusters for comparison
-- 1 Downsize, 1 NoChange (was wrong before), 1 Upsize
SELECT
    ClusterName,
    DayType,
    HourType,
    CurrentSku,
    RecommendedSku,
    Action,
    ROUND(PeakCpuMax, 2)                AS PeakCpuMax,
    ROUND(EstimatedMonthlySavings, 2)   AS Savings,
    Comment
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month    = '2026-06'
AND   DayType  = 'Weekday'
AND   HourType = 'BusinessHours'
AND   ClusterName IN (
    -- One from each action
    'cdr-uat',
    'cwih-cp-mgmt-prod',
    'cmsonc-eob-prod-cluster'
)
ORDER BY ClusterName
GO