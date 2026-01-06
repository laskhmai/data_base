/* 0) ONE-TIME DDL (run once) */
/*
ALTER TABLE [Silver].[Cloudability_Daily_Resource_Cost_Staging]
ADD humana_app_resource_map VARCHAR(4000);

ALTER TABLE [Silver].[Cloudability_Daily_Resource_Cost]
ADD humana_app_resource_map VARCHAR(4000);
*/
GO


/* 1) PROCEDURE */
CREATE OR ALTER PROC [Silver].[usp_CloudabilityAggregate_DailySpend]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ResolvedDate DATE = DATEADD(DAY, -6, CAST(GETDATE() AS DATE));

    /* A) clear staging */
    TRUNCATE TABLE [Silver].[Cloudability_Daily_Resource_Cost_Staging];

    /* B) Base */
    WITH base AS (
        SELECT
            CONVERT(date, s.[date]) AS billing_date,

            -- normalize resource_id (your STUFF logic)
            STUFF(
                s.resource_id,
                1,
                CASE WHEN CHARINDEX('/', s.resource_id) > 0
                     THEN CHARINDEX('/', s.resource_id) - 1
                     ELSE 0 END,
                ''
            ) AS resource_id,

            s.vendor_account_name,
            s.vendor,
            s.Azure_Resource_Name       AS azure_resource_name,
            s.[Azure_Resource_Group(tag11)] AS azure_resource_group,
            s.service_name,
            s.vendor_account_identifier,
            s.region,

            s.[Humana_Application_ID(tag??)] AS humana_application_id,   -- adjust tag/column if needed
            s.[Humana_Resource_ID(tag23)]    AS humana_resource_id,      -- based on your screenshot

            s.operation,
            CAST(ISNULL(s.amortized_spend, 0.0) AS DECIMAL(18,8)) AS amortized_spend,
            CAST(ISNULL(s.usage_quantity, 0.0)  AS DECIMAL(18,8)) AS usage_quantity,

            s.usage_family,
            s.usage_type,

            CONVERT(date, s.updated_date) AS updated_date
        FROM [Cloudability].[Daily_Spend] s
        WHERE s.vendor = 'Azure'
          AND CONVERT(date, s.[date]) = @ResolvedDate
    ),

    /* C) 1) Operation SUM first (this fixes duplicates like D4ds v5 repeated) */
    op_sum AS (
        SELECT
            billing_date, resource_id, vendor,
            vendor_account_name, vendor_account_identifier, region,
            azure_resource_name, azure_resource_group, service_name,
            humana_application_id, humana_resource_id,
            operation,
            SUM(amortized_spend) AS op_spend,
            SUM(usage_quantity)  AS op_usage,
            MAX(updated_date)    AS updated_date
        FROM base
        GROUP BY
            billing_date, resource_id, vendor,
            vendor_account_name, vendor_account_identifier, region,
            azure_resource_name, azure_resource_group, service_name,
            humana_application_id, humana_resource_id,
            operation
    ),

    /* D) 2) Resource level totals + operation JSON strings */
    op_rollup AS (
        SELECT
            billing_date,
            resource_id,
            vendor,

            MAX(vendor_account_name)       AS vendor_account_name,
            MAX(vendor_account_identifier) AS vendor_account_identifier,
            MAX(region)                    AS region,
            MAX(azure_resource_name)       AS azure_resource_name,
            MAX(azure_resource_group)      AS azure_resource_group,
            MAX(service_name)              AS service_name,

            -- overall totals
            SUM(op_spend) AS overall_amortized_spend,
            SUM(op_usage) AS overall_usage_quantity,

            -- ONE column JSON-like string
            '{' + STRING_AGG(
                    '"' + operation + '":' + CONVERT(VARCHAR(50), op_spend),
                    ','
                 ) + '}' AS operation_cost,

            '{' + STRING_AGG(
                    '"' + operation + '":' + CONVERT(VARCHAR(50), op_usage),
                    ','
                 ) + '}' AS operation_usage,

            MAX(updated_date) AS updated_date
        FROM op_sum
        GROUP BY billing_date, resource_id, vendor
    ),

    /* E) Usage family SUM first */
    uf_sum AS (
        SELECT
            billing_date, resource_id, vendor,
            usage_family,
            SUM(amortized_spend) AS uf_spend,
            SUM(usage_quantity)  AS uf_qty
        FROM base
        GROUP BY billing_date, resource_id, vendor, usage_family
    ),

    uf_rollup AS (
        SELECT
            billing_date, resource_id, vendor,

            '{' + STRING_AGG(
                    '"' + usage_family + '":' + CONVERT(VARCHAR(50), uf_spend),
                    ','
                 ) + '}' AS usage_family_cost,

            '{' + STRING_AGG(
                    '"' + usage_family + '":' + CONVERT(VARCHAR(50), uf_qty),
                    ','
                 ) + '}' AS usage_family_quantity
        FROM uf_sum
        GROUP BY billing_date, resource_id, vendor
    ),

    /* F) Usage types list */
    ut_rollup AS (
        SELECT
            billing_date, resource_id, vendor,
            STRING_AGG(usage_type, ',') AS usage_types
        FROM (SELECT DISTINCT billing_date, resource_id, vendor, usage_type FROM base) d
        GROUP BY billing_date, resource_id, vendor
    ),

    /* ✅ G) NEW: Humana mapping aggregation */
    humana_pairs AS (
        SELECT DISTINCT
            billing_date, resource_id, vendor,
            vendor_account_name,
            humana_application_id,
            humana_resource_id
        FROM base
        WHERE vendor_account_name IS NOT NULL
          AND humana_application_id IS NOT NULL
          AND humana_resource_id IS NOT NULL
    ),
    humana_rollup AS (
        SELECT
            billing_date, resource_id, vendor,
            '{' + STRING_AGG(
                    '"' + vendor_account_name + '|' + humana_application_id + '":"' + humana_resource_id + '"',
                    ','
                 ) + '}' AS humana_app_resource_map
        FROM humana_pairs
        GROUP BY billing_date, resource_id, vendor
    )

    /* H) Final insert into staging (1 row per resource per day) */
    INSERT INTO [Silver].[Cloudability_Daily_Resource_Cost_Staging] (
        billing_date,
        resource_id,
        vendor_account_name,
        vendor,
        overall_amortized_spend,
        operation_cost,
        operation_usage,
        overall_usage_quantity,
        azure_resource_name,
        azure_resource_group,
        service_name,
        usage_family_cost,
        usage_family_quantity,
        usage_types,
        vendor_account_identifier,
        region,
        humana_application_id,
        humana_resource_id,
        humana_app_resource_map,
        updated_date,
        last_modified_date
    )
    SELECT
        o.billing_date,
        o.resource_id,
        o.vendor_account_name,
        o.vendor,
        o.overall_amortized_spend,
        o.operation_cost,
        o.operation_usage,
        o.overall_usage_quantity,
        o.azure_resource_name,
        o.azure_resource_group,
        o.service_name,
        uf.usage_family_cost,
        uf.usage_family_quantity,
        ut.usage_types,
        o.vendor_account_identifier,
        o.region,

        NULL AS humana_application_id,   -- keep these if you still need separate cols; otherwise remove
        NULL AS humana_resource_id,

        hm.humana_app_resource_map,

        o.updated_date,
        CONVERT(date, GETDATE()) AS last_modified_date
    FROM op_rollup o
    LEFT JOIN uf_rollup uf
        ON uf.billing_date = o.billing_date
       AND uf.resource_id  = o.resource_id
       AND uf.vendor       = o.vendor
    LEFT JOIN ut_rollup ut
        ON ut.billing_date = o.billing_date
       AND ut.resource_id  = o.resource_id
       AND ut.vendor       = o.vendor
    LEFT JOIN humana_rollup hm
        ON hm.billing_date = o.billing_date
       AND hm.resource_id  = o.resource_id
       AND hm.vendor       = o.vendor;


    /* I) Update existing in main */
    UPDATE tgt
    SET
        tgt.vendor_account_name       = src.vendor_account_name,
        tgt.overall_amortized_spend   = src.overall_amortized_spend,
        tgt.operation_cost            = src.operation_cost,
        tgt.operation_usage           = src.operation_usage,
        tgt.overall_usage_quantity    = src.overall_usage_quantity,
        tgt.azure_resource_name       = src.azure_resource_name,
        tgt.azure_resource_group      = src.azure_resource_group,
        tgt.service_name              = src.service_name,
        tgt.usage_family_cost         = src.usage_family_cost,
        tgt.usage_family_quantity     = src.usage_family_quantity,
        tgt.usage_types               = src.usage_types,
        tgt.vendor_account_identifier = src.vendor_account_identifier,
        tgt.region                    = src.region,
        tgt.humana_app_resource_map   = src.humana_app_resource_map,
        tgt.updated_date              = src.updated_date,
        tgt.last_modified_date        = src.last_modified_date
    FROM [Silver].[Cloudability_Daily_Resource_Cost] tgt
    JOIN [Silver].[Cloudability_Daily_Resource_Cost_Staging] src
      ON tgt.billing_date = src.billing_date
     AND tgt.resource_id  = src.resource_id
     AND tgt.vendor       = src.vendor;


    /* J) Insert new rows into main */
    INSERT INTO [Silver].[Cloudability_Daily_Resource_Cost] (
        billing_date,
        resource_id,
        vendor_account_name,
        vendor,
        overall_amortized_spend,
        operation_cost,
        operation_usage,
        overall_usage_quantity,
        azure_resource_name,
        azure_resource_group,
        service_name,
        usage_family_cost,
        usage_family_quantity,
        usage_types,
        vendor_account_identifier,
        region,
        humana_app_resource_map,
        updated_date,
        last_modified_date
    )
    SELECT
        src.billing_date,
        src.resource_id,
        src.vendor_account_name,
        src.vendor,
        src.overall_amortized_spend,
        src.operation_cost,
        src.operation_usage,
        src.overall_usage_quantity,
        src.azure_resource_name,
        src.azure_resource_group,
        src.service_name,
        src.usage_family_cost,
        src.usage_family_quantity,
        src.usage_types,
        src.vendor_account_identifier,
        src.region,
        src.humana_app_resource_map,
        src.updated_date,
        src.last_modified_date
    FROM [Silver].[Cloudability_Daily_Resource_Cost_Staging] src
    WHERE NOT EXISTS (
        SELECT 1
        FROM [Silver].[Cloudability_Daily_Resource_Cost] tgt
        WHERE tgt.billing_date = src.billing_date
          AND tgt.resource_id  = src.resource_id
          AND tgt.vendor       = src.vendor
    );
END
GO
