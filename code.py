humana_vendor_values AS (
    SELECT
        billing_date,
        resource_id,
        vendor,
        vendor_account_identifier,
        app_resource_list =
            STRING_AGG(
                humana_application_id + ':' + humana_resource_id,
                ','
            )
    FROM humana_pairs
    GROUP BY
        billing_date,
        resource_id,
        vendor,
        vendor_account_identifier
)
humana_rollup AS (
    SELECT
        billing_date,
        resource_id,
        vendor,
        humana_app_resource_map =
            '{' + STRING_AGG(
                '"' + vendor_account_identifier + '":"' + app_resource_list + '"',
                ','
            ) + '}'
    FROM humana_vendor_values
    GROUP BY
        billing_date,
        resource_id,
        vendor
)
