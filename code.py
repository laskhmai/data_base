SELECT date, SUM(amortized_spend)
FROM cloudability.daily_spend
WHERE vendor='azure'
AND date BETWEEN '2026-05-01' AND '2026-05-10'
AND service_name NOT IN 
    ('Microsoft.Databricks','Microsoft.Synapse')
GROUP BY date


SELECT billing_date, SUM(overall_amortized_spend)
FROM [Silver].[Cloudability_Daily_Resource_Cost]
WHERE vendor='Azure'
AND billing_date BETWEEN '2026-05-01' AND '2026-05-10'
AND service_name NOT IN 
    ('Microsoft.Databricks','Microsoft.Synapse')
GROUP BY billing_date


-- Check ONE specific resource 
-- that's causing the difference
SELECT 
    s.resource_id,
    SUM(s.amortized_spend) as raw_spend,
    s.service_name
FROM [Cloudability].[Daily_Spend] s
WHERE s.vendor = 'azure'
AND CONVERT(DATE,s.[date]) = '2026-05-09'
GROUP BY s.resource_id, s.service_name
ORDER BY SUM(s.amortized_spend) DESC