-- Check what columns have data for GCP
-- Specifically resource hierarchy columns
SELECT TOP 5
     resource_id
    , vendor_account_name        -- this is like GCP Project
    , vendor_account_identifier  
    , service_name
    , region
    , [Azure_Resource_Name]      -- does this have GCP data?
    , [Azure_Resource_Group(tag11)]  -- NULL for GCP?
    , [Humana_Application_ID]
    , [Humana_Resource_ID(tag23)]
FROM [Cloudability].[Daily_Spend]
WHERE vendor = 'GCP'
  AND service_name = 'GCP Compute Engine'  -- check biggest service
  AND date >= DATEADD(DAY, -7, GETDATE())