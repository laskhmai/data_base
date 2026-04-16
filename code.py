SELECT 
    Name,
    ClusterType,
    ConnectionStrings
FROM [MongoDB].[Clusters]
WHERE CASE 
    WHEN ConnectionStrings LIKE '%azure%' THEN 'Azure'
    WHEN ConnectionStrings LIKE '%gcp%'   THEN 'GCP'
    WHEN ConnectionStrings LIKE '%eastus%'  THEN 'Azure'
    WHEN ConnectionStrings LIKE '%us-east%' THEN 'AWS'
    ELSE 'Unknown'
END = 'Unknown'