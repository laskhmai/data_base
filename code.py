-- Compare Synapse data at resource level
SELECT 
    'RAW' as source,
    CONVERT(DATE, s.[date]) as date,
    s.resource_id,
    SUM(s.amortized_spend) as spend
FROM [Cloudability].[Daily_Spend] s
WHERE s.vendor = 'azure'
AND s.[date] BETWEEN '2026-05-01' AND '2026-05-10'
AND s.service_name = 'Microsoft.Synapse'
GROUP BY CONVERT(DATE, s.[date]), s.resource_id

EXCEPT

SELECT 
    'SILVER' as source,
    billing_date,
    resource_id,
    SUM(overall_amortized_spend)
FROM [Silver].[Cloudability_Daily_Resource_Cost]
WHERE vendor = 'Azure'
AND CAST(billing_date AS DATE) 
    BETWEEN '2026-05-01' AND '2026-05-10'
AND service_name = 'Microsoft.Synapse'
GROUP BY billing_date, resource_id