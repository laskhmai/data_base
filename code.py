-- Check missing Synapse records for last 3 months
SELECT 
    CONVERT(DATE, s.[date]) as date,
    COUNT(*) as missing_records,
    SUM(s.amortized_spend) as missing_spend
FROM [Cloudability].[Daily_Spend] s
WHERE s.vendor = 'azure'
AND s.[date] BETWEEN '2026-02-01' AND '2026-05-10'
AND s.service_name = 'Microsoft.Synapse'
AND NOT EXISTS (
    SELECT 1 
    FROM [Silver].[Cloudability_Daily_Resource_Cost] t
    WHERE t.billing_date = CONVERT(DATE, s.[date])
    AND t.resource_id = s.resource_id
    AND t.vendor = 'Azure'
)
GROUP BY CONVERT(DATE, s.[date])
ORDER BY date