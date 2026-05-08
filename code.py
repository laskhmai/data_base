Reply: "Yes I can see dpk-test-cluster 
in our Clusters table. ClustersKey = X, 
StateName = IDLE. It should be getting 
picked up by the processor."

SELECT 
    ClustersKey,
    Name,
    ClusterId,
    StateName,
    CreateDate
FROM [MongoDB].[Clusters]
WHERE Name LIKE '%dpk%'
   OR Name LIKE '%dpk-test%'