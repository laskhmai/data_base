SELECT 
     CONVERT(DATE, s.[date])        AS billing_date
    , s.resource_id
    , s.vendor_account_name         AS gcp_project
    , s.vendor
    , s.service_name
    , s.region
    , s.[Azure_Resource_Name]       AS gcp_resource_name
    , s.Humana_Application_ID
    , s.[Humana_Resource_ID(tag23)]
    , s.[Operation]
    , ISNULL(s.amortized_spend, 0)  AS amortized_spend
    , ISNULL(s.usage_quantity, 0)   AS usage_quantity
    , s.usage_family
    , s.usage_type
    , s.reservation_identifier
FROM [Cloudability].[Daily_Spend] s
WHERE s.vendor = 'GCP'
  AND CONVERT(DATE, s.[date]) = '2026-07-15'  -- pick a date you know has data
ORDER BY s.resource_id