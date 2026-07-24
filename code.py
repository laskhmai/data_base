SELECT 
     -- Total resources
     COUNT(*)                                    AS total_resources

     -- Humana Application ID
    , SUM(CASE WHEN humana_applicationid_cost 
               LIKE '%"NULL"%' 
               THEN 1 ELSE 0 END)               AS missing_app_id

    , SUM(CASE WHEN humana_applicationid_cost 
               NOT LIKE '%"NULL"%' 
               THEN 1 ELSE 0 END)               AS has_app_id

     -- Humana Resource ID
    , SUM(CASE WHEN humana_resourceid_cost 
               LIKE '%"NULL"%' 
               THEN 1 ELSE 0 END)               AS missing_resource_id

    , SUM(CASE WHEN humana_resourceid_cost 
               NOT LIKE '%"NULL"%' 
               THEN 1 ELSE 0 END)               AS has_resource_id

     -- GCP Resource Name
    , SUM(CASE WHEN gcp_resource_name 
               IS NULL 
               THEN 1 ELSE 0 END)               AS missing_gcp_resource_name

    , SUM(CASE WHEN gcp_resource_name 
               IS NOT NULL 
               THEN 1 ELSE 0 END)               AS has_gcp_resource_name

     -- Region
    , SUM(CASE WHEN region IS NULL 
               OR region = '(not set)' 
               THEN 1 ELSE 0 END)               AS missing_region

FROM [Silver].[Cloudability_Daily_Resource_Cost_GCP]
WHERE billing_date = '2026-07-15'



-- Which services have most missing tags?
SELECT 
     service_name
    , COUNT(*)                                   AS total_resources
    , SUM(CASE WHEN humana_applicationid_cost 
               LIKE '%"NULL"%' 
               THEN 1 ELSE 0 END)               AS missing_app_id
    , SUM(CASE WHEN humana_resourceid_cost 
               LIKE '%"NULL"%' 
               THEN 1 ELSE 0 END)               AS missing_resource_id
    , SUM(CASE WHEN gcp_resource_name 
               IS NULL 
               THEN 1 ELSE 0 END)               AS missing_resource_name
FROM [Silver].[Cloudability_Daily_Resource_Cost_GCP]
WHERE billing_date = '2026-07-15'
GROUP BY service_name
ORDER BY total_resources DESC