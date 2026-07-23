-- How many raw rows exist for this one Silver resource_id
SELECT 
     date
    , resource_id
    , vendor
    , [Operation]
    , amortized_spend
    , usage_quantity
    , usage_family
    , service_name
FROM [Cloudability].[Daily_Spend]
WHERE vendor = 'Azure'
  AND date = '2026-07-08'
  AND resource_id LIKE '%/subscriptions/185d4a23-095e-4401-8e17-8a324c77c365/resourcegroups/adb-udap-cdp-prd-eastus2-rg/providers/microsoft.compute/disks/65e4465bd8174e6198af00301bc41c6e-0-scratchvolume%'
ORDER BY [Operation]