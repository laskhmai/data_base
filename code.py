SELECT SUM(amortized_spend) as raw_excluded
FROM [Cloudability].[Daily_Spend]
WHERE vendor = 'azure'
AND CONVERT(DATE,[date]) = '2026-05-09'
AND service_name IN 
('Microsoft.Databricks','Microsoft.Synapse')



SELECT SUM(overall_amortized_spend) as silver_excluded
FROM [Silver].[Cloudability_Daily_Resource_Cost]
WHERE vendor = 'Azure'
AND billing_date = '2026-05-09'
AND service_name IN 
('Microsoft.Databricks','Microsoft.Synapse')