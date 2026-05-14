SELECT 
    STUFF(resource_id, 1,
        CASE WHEN CHARINDEX('/', resource_id) > 0 
        THEN CHARINDEX('/', resource_id) - 1 
        ELSE 0 END, '') as resource_id,
    COUNT(DISTINCT service_name) as service_name_count,
    MAX(service_name) as max_service_name,
    MIN(service_name) as min_service_name,
    SUM(amortized_spend) as total_spend
FROM [Cloudability].[Daily_Spend]
WHERE vendor = 'azure'
AND CONVERT(DATE,[date]) = '2026-05-09'
GROUP BY 
    STUFF(resource_id, 1,
        CASE WHEN CHARINDEX('/', resource_id) > 0 
        THEN CHARINDEX('/', resource_id) - 1 
        ELSE 0 END, '')
HAVING COUNT(DISTINCT service_name) > 1
ORDER BY SUM(amortized_spend) DESC