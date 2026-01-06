DECLARE @dt date = '2025-12-19';

WITH base AS (
    SELECT
        billing_date = CONVERT(date, s.[date]),
        resource_id  = s.resource_id,
        vendor       = s.vendor,
        operation    = s.[operation],
        amortized_spend = CAST(ISNULL(s.amortized_spend,0) AS decimal(18,8)),
        usage_quantity  = CAST(ISNULL(s.usage_quantity,0)  AS decimal(18,8)),
        usage_family    = s.usage_family,
        usage_type      = s.usage_type,
        vendor_account_identifier = s.vendor_account_identifier,
        humana_application_id     = s.Humana_Application_ID,
        humana_resource_id        = s.[Humana_Resource_ID(tag23)]
    FROM [Cloudability].[Daily_Spend] s
    WHERE s.vendor = 'Azure'
      AND CONVERT(date, s.[date]) = @dt
),
op_sum AS (
    SELECT billing_date, resource_id, vendor, operation,
           op_cost  = SUM(amortized_spend),
           op_usage = SUM(usage_quantity)
    FROM base
    GROUP BY billing_date, resource_id, vendor, operation
),
usage_family_sum AS (
    SELECT billing_date, resource_id, vendor, usage_family,
           fam_cost  = SUM(amortized_spend),
           fam_qty   = SUM(usage_quantity)
    FROM base
    GROUP BY billing_date, resource_id, vendor, usage_family
),
usage_type_distinct AS (
    SELECT DISTINCT billing_date, resource_id, vendor, usage_type
    FROM base
),
humana_pairs AS (
    SELECT DISTINCT
        billing_date, resource_id, vendor,
        vendor_account_identifier, humana_application_id, humana_resource_id
    FROM base
    WHERE vendor_account_identifier IS NOT NULL
      AND humana_application_id IS NOT NULL
      AND humana_resource_id IS NOT NULL
)
SELECT TOP 50
    b.billing_date,
    b.resource_id,
    b.vendor,

    op_cost_len =
        LEN('{' + STRING_AGG(CONCAT('"', operation, '":', CONVERT(varchar(50), op_cost)), ',') + '}'),
    op_usage_len =
        LEN('{' + STRING_AGG(CONCAT('"', operation, '":', CONVERT(varchar(50), op_usage)), ',') + '}'),

    fam_cost_len =
        LEN('{' + STRING_AGG(CONCAT('"', usage_family, '":', CONVERT(varchar(50), fam_cost)), ',') + '}'),
    fam_qty_len =
        LEN('{' + STRING_AGG(CONCAT('"', usage_family, '":', CONVERT(varchar(50), fam_qty)), ',') + '}'),

    usage_types_len =
        LEN(STRING_AGG(usage_type, ',')),

    humana_rollup_len =
        LEN('{' + STRING_AGG(
                CONCAT('"', vendor_account_identifier, ':', humana_application_id, '":', '"', humana_resource_id, '"')
            , ',') + '}')

FROM (SELECT DISTINCT billing_date, resource_id, vendor FROM base) b
LEFT JOIN op_sum os
    ON os.billing_date=b.billing_date AND os.resource_id=b.resource_id AND os.vendor=b.vendor
LEFT JOIN usage_family_sum uf
    ON uf.billing_date=b.billing_date AND uf.resource_id=b.resource_id AND uf.vendor=b.vendor
LEFT JOIN usage_type_distinct ut
    ON ut.billing_date=b.billing_date AND ut.resource_id=b.resource_id AND ut.vendor=b.vendor
LEFT JOIN humana_pairs hp
    ON hp.billing_date=b.billing_date AND hp.resource_id=b.resource_id AND hp.vendor=b.vendor

GROUP BY b.billing_date, b.resource_id, b.vendor
ORDER BY
    (CASE
        WHEN LEN('{' + STRING_AGG(CONCAT('"', operation, '":', CONVERT(varchar(50), op_cost)), ',') + '}') > 8000 THEN 1
        WHEN LEN('{' + STRING_AGG(CONCAT('"', operation, '":', CONVERT(varchar(50), op_usage)), ',') + '}') > 8000 THEN 1
        WHEN LEN('{' + STRING_AGG(CONCAT('"', usage_family, '":', CONVERT(varchar(50), fam_cost)), ',') + '}') > 8000 THEN 1
        WHEN LEN('{' + STRING_AGG(CONCAT('"', usage_family, '":', CONVERT(varchar(50), fam_qty)), ',') + '}') > 8000 THEN 1
        WHEN LEN(STRING_AGG(usage_type, ',')) > 8000 THEN 1
        WHEN LEN('{' + STRING_AGG(CONCAT('"', vendor_account_identifier, ':', humana_application_id, '":', '"', humana_resource_id, '"'), ',') + '}') > 8000 THEN 1
        ELSE 0
     END) DESC,
     op_cost_len DESC;
