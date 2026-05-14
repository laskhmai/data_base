-- Find Fabric resources in Silver 
-- that are Synapse in Raw for May 9
SELECT 
    s.resource_id,
    r.service_name as raw_service_name,
    s.service_name as silver_service_name,
    r.raw_spend,
    s.silver_spend
FROM
    (SELECT resource_id,
            service_name,
            SUM(amortized_spend) as raw_spend
     FROM [Cloudability].[Daily_Spend]
     WHERE vendor = 'azure'
     AND CONVERT(DATE,[date]) = '2026-05-09'
     GROUP BY resource_id, service_name) r
JOIN
    (SELECT resource_id,
            service_name,
            SUM(overall_amortized_spend) as silver_spend
     FROM [Silver].[Cloudability_Daily_Resource_Cost]
     WHERE vendor = 'Azure'
     AND billing_date = '2026-05-09'
     AND service_name = 'Microsoft.Fabric'
     GROUP BY resource_id, service_name) s
ON r.resource_id = STUFF(s.resource_id,1,0,'')
WHERE r.service_name != s.service_name
ORDER BY r.raw_spend DESC