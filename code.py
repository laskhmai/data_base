-- CHECK 1: Row Count Silver
SELECT 'CHECK 1 - Silver Row Count' AS check_name
    , COUNT(*) AS value
FROM [Silver].[Cloudability_Daily_Resource_Cost_GCP]
WHERE billing_date = '2026-07-15'

UNION ALL

-- CHECK 2a: Raw Row Count
SELECT 'CHECK 2 - RAW Row Count' AS check_name
    , COUNT(*) AS value
FROM [Cloudability].[Daily_Spend]
WHERE vendor = 'GCP'
AND date = '2026-07-15'

UNION ALL

-- CHECK 2b: Silver Row Count
SELECT 'CHECK 2 - SILVER Row Count' AS check_name
    , COUNT(*) AS value
FROM [Silver].[Cloudability_Daily_Resource_Cost_GCP]
WHERE billing_date = '2026-07-15'

UNION ALL

-- CHECK 3a: Raw Total Spend
SELECT 'CHECK 3 - RAW Total Spend' AS check_name
    , SUM(amortized_spend) AS value
FROM [Cloudability].[Daily_Spend]
WHERE vendor = 'GCP'
AND date = '2026-07-15'

UNION ALL

-- CHECK 3b: Silver Total Spend
SELECT 'CHECK 3 - SILVER Total Spend' AS check_name
    , SUM(overall_amortized_spend) AS value
FROM [Silver].[Cloudability_Daily_Resource_Cost_GCP]
WHERE billing_date = '2026-07-15'