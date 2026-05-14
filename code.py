-- Compare spend by service_name for May 9
SELECT 
    ISNULL(r.service_name, s.service_name) as service_name,
    ISNULL(r.raw_spend, 0) as raw_spend,
    ISNULL(s.silver_spend, 0) as silver_spend,
    ISNULL(s.silver_spend, 0) - 
    ISNULL(r.raw_spend, 0) as difference
FROM
    (SELECT service_name,
            SUM(amortized_spend) as raw_spend
     FROM [Cloudability].[Daily_Spend]
     WHERE vendor = 'azure'
     AND CONVERT(DATE,[date]) = '2026-05-09'
     AND service_name NOT IN 
         ('Microsoft.Databricks','Microsoft.Synapse')
     GROUP BY service_name) r
FULL OUTER JOIN
    (SELECT service_name,
            SUM(overall_amortized_spend) as silver_spend
     FROM [Silver].[Cloudability_Daily_Resource_Cost]
     WHERE vendor = 'Azure'
     AND billing_date = '2026-05-09'
     AND service_name NOT IN 
         ('Microsoft.Databricks','Microsoft.Synapse')
     GROUP BY service_name) s
ON r.service_name = s.service_name
WHERE ISNULL(s.silver_spend,0) - 
      ISNULL(r.raw_spend,0) <> 0
ORDER BY ABS(ISNULL(s.silver_spend,0) - 
             ISNULL(r.raw_spend,0)) DESC



DECLARE @UsageDate DATE = '2026-05-09'

WITH base AS
(
    SELECT 
        billing_date = CONVERT(DATE, s.[date])
        , resource_id = STUFF(s.resource_id, 1,
            CASE WHEN CHARINDEX('/', s.resource_id) > 0 
            THEN CHARINDEX('/', s.resource_id) - 1 
            ELSE 0 END, '')
        , s.vendor_account_name
        , s.vendor
        , s.service_name
        , amortized_spend = ISNULL(s.amortized_spend, 0.0)
        , usage_quantity  = ISNULL(s.usage_quantity, 0.0)
    FROM [Cloudability].[Daily_Spend] s
    WHERE s.vendor = 'Azure'
    AND CONVERT(date, s.[date]) = @UsageDate
),
parent AS
(
    SELECT 
        b.billing_date
        , b.resource_id
        , b.vendor
        , overall_amortized_spend = SUM(b.amortized_spend)
    FROM base b
    GROUP BY 
        b.billing_date
        , b.resource_id
        , b.vendor
)
SELECT 
    SUM(overall_amortized_spend) as total_spend
FROM parent



SELECT SUM(overall_amortized_spend)
FROM [Silver].[Cloudability_Daily_Resource_Cost]
WHERE billing_date = '2026-05-09'
AND vendor = 'Azure'