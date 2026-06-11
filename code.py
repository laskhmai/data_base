-- Step 1: Check how many rows recommendations has for June
SELECT COUNT(*) AS RecommendationRows
FROM [Metrics].[MongoDBRightsizingRecommendations]
WHERE Month = '2026-06'
GO

-- Step 2: Check aggregated rows for June
SELECT COUNT(*) AS AggregatedRows
FROM [Metrics].[MongoDBRightsizingAggregated5Min]
WHERE FORMAT(_date, 'yyyy-MM') = '2026-06'
GO

-- Step 3: SIMULATE the join - see how many rows it produces
SELECT COUNT(*) AS SimulatedRows
FROM [Metrics].[MongoDBRightsizingAggregated5Min] a
INNER JOIN [Metrics].[MongoDBRightsizingRecommendations] r
    ON  r.ClusterKey = a.ClusterKey
    AND r.DayType    = a.[type]
    AND r.Month      = '2026-06'
    AND (   a.[type]    = 'Weekend'
         OR r.HourType  = a.businessHour)
WHERE FORMAT(a._date, 'yyyy-MM') = '2026-06'
GO