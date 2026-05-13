SELECT 
    CONVERT(DATE, s.[date]) as date,
    SUM(s.amortized_spend) as raw_spend,
    SUM(CAST(ISNULL(s.amortized_spend, 0.0) 
        AS DECIMAL(18,8))) as current_proc_spend,
    SUM(s.amortized_spend) - 
    SUM(CAST(ISNULL(s.amortized_spend, 0.0) 
        AS DECIMAL(18,8))) as difference
FROM [Cloudability].[Daily_Spend] s
WHERE s.vendor = 'Azure'
AND s.[date] BETWEEN '2026-05-01' AND '2026-05-10'
GROUP BY CONVERT(DATE, s.[date])
ORDER BY date


SELECT 
    CONVERT(DATE, s.[date]) as date,
    SUM(s.amortized_spend) as raw_spend,
    SUM(ISNULL(s.amortized_spend, 0.0)) as fixed_spend,
    SUM(s.amortized_spend) - 
    SUM(ISNULL(s.amortized_spend, 0.0)) as difference
FROM [Cloudability].[Daily_Spend] s
WHERE s.vendor = 'Azure'
AND s.[date] BETWEEN '2026-05-01' AND '2026-05-10'
GROUP BY CONVERT(DATE, s.[date])
ORDER BY date