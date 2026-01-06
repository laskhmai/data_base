/*=====================================================================================
  DROP + CREATE pattern (Synapse Dedicated SQL Pool / PDW does NOT support CREATE OR ALTER)
=====================================================================================*/
IF OBJECT_ID('[Silver].[usp_CloudabilityAggregate_DailySpend]') IS NOT NULL
    DROP PROCEDURE [Silver].[usp_CloudabilityAggregate_DailySpend];
GO

CREATE PROCEDURE [Silver].[usp_CloudabilityAggregate_DailySpend]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ResolvedDate date = DATEADD(DAY, -6, CAST(GETDATE() AS date));   -- change if you want

    BEGIN TRY

        /* 1) Reset staging (staging can be truncated – main table is NOT deleted) */
        TRUNCATE TABLE [Silver].[Cloudability_Daily_Resource_Cost_Staging];

        /* 2) Build ONE row per (billing_date, resource_id, vendor) with rollup JSON columns */
        ;WITH base AS
        (
            SELECT
                billing_date = CONVERT(date, s.[date]),

                /* keep the same logic you had: strip everything up to first '/' */
                resource_id =
                    STUFF(
                        s.resource_id,
                        1,
                        CASE
                            WHEN CHARINDEX('/', s.resource_id) > 0 THEN CHARINDEX('/', s.resource_id) - 1
                            ELSE 0
                        END,
                        ''
                    ),

                s.vendor_account_name,
                s.vendor,
                s.vendor_account_identifier,
                s.region,

                azure_resource_name  = s.Azure_Resource_Name,
                azure_resource_group = s.[Azure_Resource_Group(tag11)],
                s.service_name,

                s.operation,
                s.usage_type,
                s.usage_family,
                s.reservation_identifier,

                humana_application_id = s.Humana_Application_ID,
                humana_resource_id    = s.[Humana_Resource_ID(tag23)],

                amortized_spend = CAST(ISNULL(s.amortized_spend, 0.0) AS decimal(18,8)),
                usage_quantity  = CAST(ISNULL(s.usage_quantity, 0.0)  AS decimal(18,8)),

                updated_date = CONVERT(date, s.updated_date)
            FROM [Cloudability].[Daily_Spend] s
            WHERE s.vendor = 'Azure'
              AND CONVERT(date, s.[date]) = @ResolvedDate
        ),

        /* =========================
           OPERATION COST rollup
           ========================= */
        op_cost_sum AS
        (
            SELECT
                billing_date, resource_id, vendor,
                op_key   = COALESCE([operation], 'NULL'),
                op_spend = SUM(amortized_spend)
            FROM base
            GROUP BY billing_date, resource_id, vendor, COALESCE([operation], 'NULL')
        ),
        op_cost_rollup AS
        (
            SELECT
                billing_date, resource_id, vendor,
                operation_cost =
                    '{' + STRING_AGG(
                        CAST(CONCAT('"', op_key, '":', CONVERT(varchar(50), op_spend)) AS nvarchar(max)),
                        ','
                    ) + '}'
            FROM op_cost_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* =========================
           OPERATION USAGE rollup
           ========================= */
        op_usage_sum AS
        (
            SELECT
                billing_date, resource_id, vendor,
                op_key    = COALESCE([operation], 'NULL'),
                op_qtysum = SUM(usage_quantity)
            FROM base
            GROUP BY billing_date, resource_id, vendor, COALESCE([operation], 'NULL')
        ),
        op_usage_rollup AS
        (
            SELECT
                billing_date, resource_id, vendor,
                operation_usage =
                    '{' + STRING_AGG(
                        CAST(CONCAT('"', op_key, '":', CONVERT(varchar(50), op_qtysum)) AS nvarchar(max)),
                        ','
                    ) + '}'
            FROM op_usage_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* =========================
           USAGE FAMILY COST rollup
           ========================= */
        uf_cost_sum AS
        (
            SELECT
                billing_date, resource_id, vendor,
                uf_key   = COALESCE(usage_family, 'NULL'),
                uf_spend = SUM(amortized_spend)
            FROM base
            GROUP BY billing_date, resource_id, vendor, COALESCE(usage_family, 'NULL')
        ),
        uf_cost_rollup AS
        (
            SELECT
                billing_date, resource_id, vendor,
                usage_family_cost =
                    '{' + STRING_AGG(
                        CAST(CONCAT('"', uf_key, '":', CONVERT(varchar(50), uf_spend)) AS nvarchar(max)),
                        ','
                    ) + '}'
            FROM uf_cost_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* =========================
           USAGE FAMILY QUANTITY rollup
           ========================= */
        uf_qty_sum AS
        (
            SELECT
                billing_date, resource_id, vendor,
                uf_key  = COALESCE(usage_family, 'NULL'),
                uf_qty  = SUM(usage_quantity)
            FROM base
            GROUP BY billing_date, resource_id, vendor, COALESCE(usage_family, 'NULL')
        ),
        uf_qty_rollup AS
        (
            SELECT
                billing_date, resource_id, vendor,
                usage_family_quantity =
                    '{' + STRING_AGG(
                        CAST(CONCAT('"', uf_key, '":', CONVERT(varchar(50), uf_qty)) AS nvarchar(max)),
                        ','
                    ) + '}'
            FROM uf_qty_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* =========================
           RESERVATION IDENTIFIER COST rollup (new)
           ========================= */
        ri_sum AS
        (
            SELECT
                billing_date, resource_id, vendor,
                ri_key   = COALESCE(reservation_identifier, 'NULL'),
                ri_spend = SUM(amortized_spend)
            FROM base
            GROUP BY billing_date, resource_id, vendor, COALESCE(reservation_identifier, 'NULL')
        ),
        ri_rollup AS
        (
            SELECT
                billing_date, resource_id, vendor,
                reservation_identifier_cost =
                    '{' + STRING_AGG(
                        CAST(CONCAT('"', ri_key, '":', CONVERT(varchar(50), ri_spend)) AS nvarchar(max)),
                        ','
                    ) + '}'
            FROM ri_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* =========================
           HUMANA APPLICATION COST rollup (new)
           ========================= */
        app_sum AS
        (
            SELECT
                billing_date, resource_id, vendor,
                app_key   = COALESCE(humana_application_id, 'NULL'),
                app_spend = SUM(amortized_spend)
            FROM base
            GROUP BY billing_date, resource_id, vendor, COALESCE(humana_application_id, 'NULL')
        ),
        app_rollup AS
        (
            SELECT
                billing_date, resource_id, vendor,
                humana_application_cost =
                    '{' + STRING_AGG(
                        CAST(CONCAT('"', app_key, '":', CONVERT(varchar(50), app_spend)) AS nvarchar(max)),
                        ','
                    ) + '}'
            FROM app_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* =========================
           HUMANA RESOURCE COST rollup (new)
           ========================= */
        hr_sum AS
        (
            SELECT
                billing_date, resource_id, vendor,
                hr_key   = COALESCE(humana_resource_id, 'NULL'),
                hr_spend = SUM(amortized_spend)
            FROM base
            GROUP BY billing_date, resource_id, vendor, COALESCE(humana_resource_id, 'NULL')
        ),
        hr_rollup AS
        (
            SELECT
                billing_date, resource_id, vendor,
                humana_resource_cost =
                    '{' + STRING_AGG(
                        CAST(CONCAT('"', hr_key, '":', CONVERT(varchar(50), hr_spend)) AS nvarchar(max)),
                        ','
                    ) + '}'
            FROM hr_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* =========================
           USAGE TYPES list (distinct, then concat)
           ========================= */
        usage_type_distinct AS
        (
            SELECT DISTINCT
                billing_date, resource_id, vendor,
                usage_type = COALESCE(usage_type, 'NULL')
            FROM base
        ),
        usage_type_rollup AS
        (
            SELECT
                billing_date, resource_id, vendor,
                usage_types = STRING_AGG(CAST(usage_type AS nvarchar(max)), ',')
            FROM usage_type_distinct
            GROUP BY billing_date, resource_id, vendor
        ),

        /* =========================
           Parent row: ONE per day/resource/vendor
           ========================= */
        parent AS
        (
            SELECT
                b.billing_date,
                b.resource_id,
                b.vendor,

                vendor_account_name       = MAX(b.vendor_account_name),
                overall_amortized_spend   = SUM(b.amortized_spend),
                overall_usage_quantity    = SUM(b.usage_quantity),

                azure_resource_name       = MAX(b.azure_resource_name),
                azure_resource_group      = MAX(b.azure_resource_group),
                service_name              = MAX(b.service_name),

                vendor_account_identifier = MAX(b.vendor_account_identifier),
                region                    = MAX(b.region),

                /* keep raw columns if your table has them (optional) */
                humana_application_id     = MAX(b.humana_application_id),
                humana_resource_id        = MAX(b.humana_resource_id),

                updated_date              = MAX(b.updated_date),
                last_modified_date        = CONVERT(date, GETDATE())
            FROM base b
            GROUP BY b.billing_date, b.resource_id, b.vendor
        )

        INSERT INTO [Silver].[Cloudability_Daily_Resource_Cost_Staging]
        (
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
            updated_date,
            last_modified_date,

            /* NEW columns (make sure staging has these columns) */
            reservation_identifier_cost,
            humana_application_cost,
            humana_resource_cost
        )
        SELECT
            p.billing_date,
            p.resource_id,
            p.vendor_account_name,
            p.vendor,
            p.overall_amortized_spend,

            oc.operation_cost,
            ou.operation_usage,

            p.overall_usage_quantity,
            p.azure_resource_name,
            p.azure_resource_group,
            p.service_name,

            ufc.usage_family_cost,
            ufq.usage_family_quantity,

            ut.usage_types,

            p.vendor_account_identifier,
            p.region,
            p.humana_application_id,
            p.humana_resource_id,
            p.updated_date,
            p.last_modified_date,

            ri.reservation_identifier_cost,
            ar.humana_application_cost,
            hr.humana_resource_cost
        FROM parent p
        LEFT JOIN op_cost_rollup      oc ON oc.billing_date = p.billing_date AND oc.resource_id = p.resource_id AND oc.vendor = p.vendor
        LEFT JOIN op_usage_rollup     ou ON ou.billing_date = p.billing_date AND ou.resource_id = p.resource_id AND ou.vendor = p.vendor
        LEFT JOIN uf_cost_rollup     ufc ON ufc.billing_date = p.billing_date AND ufc.resource_id = p.resource_id AND ufc.vendor = p.vendor
        LEFT JOIN uf_qty_rollup      ufq ON ufq.billing_date = p.billing_date AND ufq.resource_id = p.resource_id AND ufq.vendor = p.vendor
        LEFT JOIN usage_type_rollup   ut ON ut.billing_date = p.billing_date AND ut.resource_id = p.resource_id AND ut.vendor = p.vendor

        LEFT JOIN ri_rollup           ri ON ri.billing_date = p.billing_date AND ri.resource_id = p.resource_id AND ri.vendor = p.vendor
        LEFT JOIN app_rollup          ar ON ar.billing_date = p.billing_date AND ar.resource_id = p.resource_id AND ar.vendor = p.vendor
        LEFT JOIN hr_rollup           hr ON hr.billing_date = p.billing_date AND hr.resource_id = p.resource_id AND hr.vendor = p.vendor
        ;

        /* 3) Update main table (NO deletes) */
        UPDATE tgt
           SET tgt.vendor_account_name        = src.vendor_account_name,
               tgt.overall_amortized_spend    = src.overall_amortized_spend,
               tgt.operation_cost             = src.operation_cost,
               tgt.operation_usage            = src.operation_usage,
               tgt.overall_usage_quantity     = src.overall_usage_quantity,
               tgt.azure_resource_name        = src.azure_resource_name,
               tgt.azure_resource_group       = src.azure_resource_group,
               tgt.service_name               = src.service_name,
               tgt.usage_family_cost          = src.usage_family_cost,
               tgt.usage_family_quantity      = src.usage_family_quantity,
               tgt.usage_types                = src.usage_types,
               tgt.vendor_account_identifier  = src.vendor_account_identifier,
               tgt.region                     = src.region,
               tgt.humana_application_id      = src.humana_application_id,
               tgt.humana_resource_id         = src.humana_resource_id,
               tgt.updated_date               = src.updated_date,
               tgt.last_modified_date         = src.last_modified_date,

               /* NEW columns */
               tgt.reservation_identifier_cost = src.reservation_identifier_cost,
               tgt.humana_application_cost     = src.humana_application_cost,
               tgt.humana_resource_cost        = src.humana_resource_cost
        FROM [Silver].[Cloudability_Daily_Resource_Cost] tgt
        JOIN [Silver].[Cloudability_Daily_Resource_Cost_Staging] src
          ON tgt.billing_date = src.billing_date
         AND tgt.resource_id  = src.resource_id
         AND tgt.vendor       = src.vendor;

        /* 4) Insert missing rows */
        INSERT INTO [Silver].[Cloudability_Daily_Resource_Cost]
        (
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
            updated_date,
            last_modified_date,

            /* NEW columns */
            reservation_identifier_cost,
            humana_application_cost,
            humana_resource_cost
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
            src.humana_application_id,
            src.humana_resource_id,
            src.updated_date,
            src.last_modified_date,

            src.reservation_identifier_cost,
            src.humana_application_cost,
            src.humana_resource_cost
        FROM [Silver].[Cloudability_Daily_Resource_Cost_Staging] src
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM [Silver].[Cloudability_Daily_Resource_Cost] tgt
            WHERE tgt.billing_date = src.billing_date
              AND tgt.resource_id  = src.resource_id
              AND tgt.vendor       = src.vendor
        );

    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END
GO
