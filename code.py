-- Check ALL variations of Databricks/Synapse
-- in BOTH tables
SELECT 
    'RAW' as source,
    service_name,
    SUM(amortized_spend) as spend
FROM [Cloudability].[Daily_Spend]
WHERE vendor = 'azure'
AND [date] BETWEEN '2026-05-01' AND '2026-05-10'
AND LOWER(service_name) LIKE '%databricks%' 
 OR LOWER(service_name) LIKE '%synapse%'
GROUP BY service_name

UNION ALL

SELECT 
    'SILVER' as source,
    service_name,
    SUM(overall_amortized_spend) as spend
FROM [Silver].[Cloudability_Daily_Resource_Cost]
WHERE vendor = 'Azure'
AND CAST(billing_date AS DATE) 
    BETWEEN '2026-05-01' AND '2026-05-10'
AND (LOWER(service_name) LIKE '%databricks%'
 OR LOWER(service_name) LIKE '%synapse%')
GROUP BY service_name
ORDER BY source, service_name