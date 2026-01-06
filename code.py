;WITH op_sum AS (
    SELECT
        CONVERT(date, s.[date]) AS billing_date,
        resource_id = STUFF(
            s.resource_id, 1,
            CASE WHEN CHARINDEX('/', s.resource_id) > 0 THEN CHARINDEX('/', s.resource_id) - 1 ELSE 0 END,
            ''
        ),
        s.vendor,
        s.[Operation],
        op_spend = SUM(CAST(ISNULL(s.amortized_spend,0.0) AS DECIMAL(18,8)))
    FROM [Cloudability].[Daily_Spend] s
    WHERE CONVERT(date, s.[date]) = @BillingDate
    GROUP BY
        CONVERT(date, s.[date]),
        STUFF(
            s.resource_id, 1,
            CASE WHEN CHARINDEX('/', s.resource_id) > 0 THEN CHARINDEX('/', s.resource_id) - 1 ELSE 0 END,
            ''
        ),
        s.vendor,
        s.[Operation]
)
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
