SELECT 
    a.date,
    a.raw_spend,
    b.silver_spend,
    b.silver_spend - a.raw_spend AS difference
FROM
    (SELECT CONVERT(DATE, s.[date]) as date,
            SUM(s.amortized_spend) as raw_spend
     FROM [Cloudability].[Daily_Spend] s
     WHERE s.vendor = 'azure'
     AND CONVERT(DATE, s.[date]) = '2026-05-09'
     AND s.service_name NOT IN 
         ('Microsoft.Databricks','Microsoft.Synapse')
     GROUP BY CONVERT(DATE, s.[date])) a
JOIN
    (SELECT billing_date,
            SUM(overall_amortized_spend) as silver_spend
     FROM [Silver].[Cloudability_Daily_Resource_Cost]
     WHERE vendor = 'Azure'
     AND billing_date = '2026-05-09'
     AND service_name NOT IN 
         ('Microsoft.Databricks','Microsoft.Synapse')
     GROUP BY billing_date) b
ON a.date = b.billing_date