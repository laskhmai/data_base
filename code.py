SELECT 
     service_name
    , COUNT(DISTINCT resource_id) AS resource_count
    , SUM(amortized_spend)        AS total_spend
FROM [Cloudability].[Daily_Spend]
WHERE vendor = 'GCP'
  AND date >= DATEADD(DAY, -7, GETDATE())
GROUP BY service_name
ORDER BY total_spend DESC