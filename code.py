SELECT 
    billing_date,
    SUM(overall_amortized_spend) as silver_spend
FROM [Silver].[Cloudability_Daily_Resource_Cost]
WHERE billing_date BETWEEN '2026-05-01' AND '2026-05-10'
GROUP BY billing_date
ORDER BY billing_date



SELECT 
    CONVERT(DATE, s.[date]) as date,
    SUM(s.amortized_spend) as raw_spend
FROM [Cloudability].[Daily_Spend] s
WHERE s.vendor = 'Azure'
AND s.[date] BETWEEN '2026-05-01' AND '2026-05-10'
GROUP BY CONVERT(DATE, s.[date])
ORDER BY 1


SELECT 
    a.date,
    a.raw_spend,
    b.silver_spend,
    b.silver_spend - a.raw_spend AS difference
FROM
    (SELECT CONVERT(DATE, s.[date]) as date,
            SUM(s.amortized_spend) as raw_spend
     FROM [Cloudability].[Daily_Spend] s
     WHERE s.vendor = 'Azure'
     AND s.[date] BETWEEN '2026-05-01' AND '2026-05-10'
     GROUP BY CONVERT(DATE, s.[date])) a
JOIN
    (SELECT billing_date,
            SUM(overall_amortized_spend) as silver_spend
     FROM [Silver].[Cloudability_Daily_Resource_Cost]
     WHERE billing_date BETWEEN '2026-05-01' AND '2026-05-10'
     GROUP BY billing_date) b
ON a.date = b.billing_date
ORDER BY a.date