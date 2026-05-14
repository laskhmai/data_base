SELECT resource_id, service_name,
       SUM(amortized_spend) as spend
FROM [Cloudability].[Daily_Spend]
WHERE vendor = 'azure'
AND CONVERT(DATE,[date]) = '2026-05-09'
AND resource_id IN (
    SELECT DISTINCT resource_id
    FROM [Cloudability].[Daily_Spend]
    WHERE vendor = 'azure'
    AND CONVERT(DATE,[date]) = '2026-05-09'
    AND service_name IN 
        ('Microsoft.Databricks','Microsoft.Synapse')
)
GROUP BY resource_id, service_name
ORDER BY resource_id, service_name