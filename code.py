-- See all unique service names 
-- that contain synapse or databricks
SELECT DISTINCT service_name,
       COUNT(*) as row_count,
       SUM(amortized_spend) as total_spend
FROM [Cloudability].[Daily_Spend]
WHERE vendor = 'azure'
AND CONVERT(DATE,[date]) = '2026-05-09'
AND (LOWER(service_name) LIKE '%synapse%'
OR LOWER(service_name) LIKE '%databricks%')
GROUP BY service_name
ORDER BY service_name

SELECT DISTINCT service_name,
       COUNT(*) as row_count,
       SUM(overall_amortized_spend) as total_spend
FROM [Silver].[Cloudability_Daily_Resource_Cost]
WHERE vendor = 'Azure'
AND billing_date = '2026-05-09'
AND (LOWER(service_name) LIKE '%synapse%'
OR LOWER(service_name) LIKE '%databricks%')
GROUP BY service_name
ORDER BY service_name