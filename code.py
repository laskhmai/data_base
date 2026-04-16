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


CASE 
    WHEN ConnectionStrings LIKE '%azure%'    THEN 'Azure'
    WHEN ConnectionStrings LIKE '%gcp%'      THEN 'GCP'
    WHEN ConnectionStrings LIKE '%mongodb.net%' 
     AND ConnectionStrings NOT LIKE '%azure%' 
     AND ConnectionStrings NOT LIKE '%gcp%'  THEN 'AWS'
    WHEN ConnectionStrings IS NULL           THEN 'No Connection String'
    ELSE 'Unknown'
END AS Provider