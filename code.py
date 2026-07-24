-- Check what max values exist in GCP
SELECT 
     MAX(amortized_spend)  AS max_spend
    , MIN(amortized_spend) AS min_spend
    , MAX(usage_quantity)  AS max_qty
    , MIN(usage_quantity)  AS min_qty
FROM [Cloudability].[Daily_Spend]
WHERE vendor = 'GCP'
AND date = '2026-07-15'