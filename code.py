-- Raw with LOWER filter
SELECT 
    CONVERT(DATE, s.[date]) as date,
    SUM(s.amortized_spend) as raw_spend
FROM [Cloudability].[Daily_Spend] s
WHERE s.vendor = 'azure'
AND s.[date] BETWEEN '2026-05-01' AND '2026-05-10'
AND LOWER(s.service_name) NOT IN 
    ('microsoft.databricks','microsoft.synapse')
GROUP BY CONVERT(DATE, s.[date])
ORDER BY date

-- Silver with LOWER filter
SELECT 
    billing_date,
    SUM(overall_amortized_spend) as silver_spend
FROM [Silver].[Cloudability_Daily_Resource_Cost]
WHERE vendor = 'Azure'
AND CAST(billing_date AS DATE) 
    BETWEEN '2026-05-01' AND '2026-05-10'
AND LOWER(service_name) NOT IN 
    ('microsoft.databricks','microsoft.synapse')
GROUP BY billing_date
ORDER BY billing_date