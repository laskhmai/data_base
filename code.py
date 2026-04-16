SELECT 
    Name,
    ClusterType,
    CASE 
        WHEN ConnectionStrings LIKE '%azure%'    THEN 'Azure'
        WHEN ConnectionStrings LIKE '%gcp%'      THEN 'GCP'
        WHEN ConnectionStrings LIKE '%mongodb.net%' 
         AND ConnectionStrings NOT LIKE '%azure%' 
         AND ConnectionStrings NOT LIKE '%gcp%'  THEN 'AWS'
        WHEN ConnectionStrings IS NULL           THEN 'No Connection String'
        ELSE 'Unknown'
    END AS Provider,
    CASE
        WHEN ConnectionStrings LIKE '%eastus2%'    THEN 'East US 2'
        WHEN ConnectionStrings LIKE '%eastus%'     THEN 'East US'
        WHEN ConnectionStrings LIKE '%centralus%'  THEN 'Central US'
        WHEN ConnectionStrings LIKE '%westus2%'    THEN 'West US 2'
        WHEN ConnectionStrings LIKE '%westus%'     THEN 'West US'
        WHEN ConnectionStrings LIKE '%us-east4%'   THEN 'GCP US East 4'
        WHEN ConnectionStrings LIKE '%us-east1%'   THEN 'GCP US East 1'
        WHEN ConnectionStrings LIKE '%us-east%'    THEN 'AWS US East'
        WHEN ConnectionStrings IS NULL             THEN 'No Connection String'
        ELSE 'Unknown'
    END AS Region
FROM [MongoDB].[Clusters]
ORDER BY Provider, Region