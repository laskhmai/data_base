SELECT 
    s.service_name,
    COUNT(*) as missing_records,
    SUM(s.amortized_spend) as missing_spend
FROM [Cloudability].[Daily_Spend] s
WHERE s.vendor = 'azure'
AND s.[date] BETWEEN '2026-05-01' AND '2026-05-10'
AND NOT EXISTS (
    SELECT 1 
    FROM [Silver].[Cloudability_Daily_Resource_Cost] t
    WHERE t.billing_date = CONVERT(DATE, s.[date])
    AND t.resource_id = STUFF(s.resource_id, 1,
        CASE WHEN CHARINDEX('/', s.resource_id) > 0 
        THEN CHARINDEX('/', s.resource_id) - 1 
        ELSE 0 END, '')
    AND t.vendor = 'Azure'
)
GROUP BY s.service_name
ORDER BY missing_spend DESC