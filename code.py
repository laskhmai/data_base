SELECT 
    s.resource_id,
    r.service_name as raw_service_name,
    s.service_name as silver_service_name,
    r.raw_spend,
    s.silver_spend
FROM
    (SELECT 
        -- Transform resource_id same way proc does
        STUFF(resource_id, 1,
            CASE WHEN CHARINDEX('/', resource_id) > 0 
            THEN CHARINDEX('/', resource_id) - 1 
            ELSE 0 END, '') as resource_id,
        service_name,
        SUM(amortized_spend) as raw_spend
     FROM [Cloudability].[Daily_Spend]
     WHERE vendor = 'azure'
     AND CONVERT(DATE,[date]) = '2026-05-09'
     GROUP BY 
        STUFF(resource_id, 1,
            CASE WHEN CHARINDEX('/', resource_id) > 0 
            THEN CHARINDEX('/', resource_id) - 1 
            ELSE 0 END, ''),
        service_name) r
JOIN
    (SELECT resource_id,
            service_name,
            SUM(overall_amortized_spend) as silver_spend
     FROM [Silver].[Cloudability_Daily_Resource_Cost]
     WHERE vendor = 'Azure'
     AND billing_date = '2026-05-09'
     AND service_name = 'Microsoft.Fabric'
     GROUP BY resource_id, service_name) s
ON r.resource_id = s.resource_id
WHERE r.service_name != s.service_name
ORDER BY r.raw_spend DESC