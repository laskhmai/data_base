-- Step 1: aggregate spend per operation (per resource/date/vendor)
WITH op_sum AS (
    SELECT
        CONVERT(date, s.[date]) AS billing_date,
        -- keep your same resource_id normalization
        STUFF(
            s.resource_id,
            1,
            CASE WHEN CHARINDEX('/', s.resource_id) > 0 THEN CHARINDEX('/', s.resource_id) - 1 ELSE 0 END,
            ''
        ) AS resource_id,
        s.vendor,
        s.[Operation],
        SUM(CAST(ISNULL(s.amortized_spend, 0.0) AS DECIMAL(18,8))) AS op_spend
    FROM [Cloudability].[Daily_Spend] s
    WHERE CONVERT(date, s.[date]) = @BillingDate   -- or your filter
    GROUP BY
        CONVERT(date, s.[date]),
        STUFF(
            s.resource_id,
            1,
            CASE WHEN CHARINDEX('/', s.resource_id) > 0 THEN CHARINDEX('/', s.resource_id) - 1 ELSE 0 END,
            ''
        ),
        s.vendor,
        s.[Operation]
)

-- Step 2: build operation_cost string using STRING_AGG on the SUMMED rows
SELECT
    billing_date,
    resource_id,
    vendor,
    operation_cost =
        '{' + STRING_AGG(
                CONCAT('"', [Operation], '":', CONVERT(VARCHAR(50), op_spend)),
                ','
             ) + '}'
FROM op_sum
GROUP BY billing_date, resource_id, vendor;
