-- Check what service names exist in Silver 
-- that contain Databricks or Synapse
SELECT DISTINCT service_name, 
       SUM(overall_amortized_spend) as spend
FROM [Silver].[Cloudability_Daily_Resource_Cost]
WHERE vendor = 'Azure'
AND CAST(billing_date AS DATE) 
    BETWEEN '2026-05-01' AND '2026-05-10'
AND service_name LIKE '%Databricks%' 
 OR service_name LIKE '%Synapse%'
GROUP BY service_name
ORDER BY spend DESC