/* =====================================================================================
   PURPOSE
   - Build 1 row per (billing_date + resource_id + vendor)
   - Fix STRING_AGG 8000-byte error by:
       1) first SUM by key (Operation / Usage_Family) to remove duplicates
       2) then STRING_AGG on the already-summed rows (much smaller output)
   - DO NOT delete from main table; only UPDATE existing + INSERT new
   - Fix THROW error-number issue by using plain THROW; (rethrow original)
===================================================================================== */

CREATE OR ALTER PROC [Silver].[usp_CloudabilityAggregate_DailySpend]
    @BillingDate DATE = NULL   -- optional; if NULL runs for GETDATE()-6
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -------------------------------------------------------------------------
        -- 0) Resolve run date
        -------------------------------------------------------------------------
        DECLARE @ResolvedDate DATE = ISNULL(@BillingDate, DATEADD(DAY, -6, CAST(GETDATE() AS DATE)));

        -------------------------------------------------------------------------
        -- 1) Clear staging (OK to truncate staging; team said don't delete main)
        -------------------------------------------------------------------------
        TRUNCATE TABLE [Silver].[Cloudability_Daily_Resource_Cost_Staging];

        -------------------------------------------------------------------------
        -- 2) Base rows (source) - normalize resource_id (remove leading '/')
        --    NOTE: adjust source table/column names if yours differ.
        -------------------------------------------------------------------------
        ;WITH base AS
        (
            SELECT
                billing_date = CONVERT(date, s.[date]),
                resource_id  = CASE WHEN LEFT(s.resource_id,1) = '/' THEN SUBSTRING(s.resource_id,2,8000) ELSE s.resource_id END,
                vendor_account_name = s.vendor_account_name,
                vendor = s.vendor,
                azure_resource_name = s.Azure_Resource_Name,
                azure_resource_group = s.[Azure_Resource_Group(tag11)],
                service_name = s.service_name,
                vendor_account_identifier = s.vendor_account_identifier,
                region = s.region,

                -- dims for the “big string” columns
                Operation = s.[Operation],
                usage_family = s.usage_family,
                usage_type = s.usage_type,

                -- measures
                amortized_spend = CAST(ISNULL(s.amortized_spend,0.0) AS DECIMAL(18,8)),
                usage_quantity  = CAST(ISNULL(s.usage_quantity ,0.0) AS DECIMAL(18,8)),

                -- attributes that sometimes vary (keep a deterministic pick)
                humana_application_id = s.Humana_Application_ID,
                humana_resource_id    = s.[Humana_Resource_ID(tag23)],
                updated_date          = CONVERT(date, s.updated_date)
            FROM [Cloudability].[Daily_Spend] s
            WHERE s.vendor = 'Azure'
              AND CONVERT(date, s.[date]) = @ResolvedDate
        ),

        -------------------------------------------------------------------------
        -- 3) Core “1 row per resource per date” rollup (numeric totals + stable attrs)
        --    IMPORTANT: we do NOT group by Operation/usage_family/usage_type here.
        -------------------------------------------------------------------------
        core AS
        (
            SELECT
                b.billing_date,
                b.resource_id,
                b.vendor,

                -- stable attrs (use MAX/MIN; adjust if you prefer)
                vendor_account_name      = MAX(b.vendor_account_name),
                azure_resource_name      = MAX(b.azure_resource_name),
                azure_resource_group     = MAX(b.azure_resource_group),
                service_name             = MAX(b.service_name),
                vendor_account_identifier= MAX(b.vendor_account_identifier),
                region                   = MAX(b.region),

                -- totals
                overall_amortized_spend  = SUM(b.amortized_spend),
                overall_usage_quantity   = SUM(b.usage_quantity),

                -- attributes that may vary across rows (pick something deterministic)
                humana_application_id    = MAX(b.humana_application_id),
                humana_resource_id       = MAX(b.humana_resource_id),

                updated_date             = MAX(b.updated_date),
                last_modified_date       = CONVERT(date, GETDATE())
            FROM base b
            GROUP BY b.billing_date, b.resource_id, b.vendor
        ),

        -------------------------------------------------------------------------
        -- 4) operation_cost: SUM amortized_spend per Operation, then JSON stringify
        -------------------------------------------------------------------------
        op_cost_sum AS
        (
            SELECT
                b.billing_date, b.resource_id, b.vendor,
                b.Operation,
                op_cost = SUM(b.amortized_spend)
            FROM base b
            WHERE b.Operation IS NOT NULL
            GROUP BY b.billing_date, b.resource_id, b.vendor, b.Operation
        ),
        op_cost_json AS
        (
            SELECT
                billing_date, resource_id, vendor,
                operation_cost =
                    '{' + STRING_AGG(
                            CONCAT('"', REPLACE(Operation,'"','\"'), '":', CONVERT(VARCHAR(50), op_cost)),
                            ','
                         ) + '}'
            FROM op_cost_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        -------------------------------------------------------------------------
        -- 5) operation_usage: SUM usage_quantity per Operation, then JSON stringify
        -------------------------------------------------------------------------
        op_usage_sum AS
        (
            SELECT
                b.billing_date, b.resource_id, b.vendor,
                b.Operation,
                op_usage = SUM(b.usage_quantity)
            FROM base b
            WHERE b.Operation IS NOT NULL
            GROUP BY b.billing_date, b.resource_id, b.vendor, b.Operation
        ),
        op_usage_json AS
        (
            SELECT
                billing_date, resource_id, vendor,
                operation_usage =
                    '{' + STRING_AGG(
                            CONCAT('"', REPLACE(Operation,'"','\"'), '":', CONVERT(VARCHAR(50), op_usage)),
                            ','
                         ) + '}'
            FROM op_usage_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        -------------------------------------------------------------------------
        -- 6) usage_family_cost: SUM amortized_spend per usage_family, then JSON
        -------------------------------------------------------------------------
        uf_cost_sum AS
        (
            SELECT
                b.billing_date, b.resource_id, b.vendor,
                b.usage_family,
                uf_cost = SUM(b.amortized_spend)
            FROM base b
            WHERE b.usage_family IS NOT NULL
            GROUP BY b.billing_date, b.resource_id, b.vendor, b.usage_family
        ),
        uf_cost_json AS
        (
            SELECT
                billing_date, resource_id, vendor,
                usage_family_cost =
                    '{' + STRING_AGG(
                            CONCAT('"', REPLACE(usage_family,'"','\"'), '":', CONVERT(VARCHAR(50), uf_cost)),
                            ','
                         ) + '}'
            FROM uf_cost_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        -------------------------------------------------------------------------
        -- 7) usage_family_quantity: SUM usage_quantity per usage_family, then JSON
        -------------------------------------------------------------------------
        uf_qty_sum AS
        (
            SELECT
                b.billing_date, b.resource_id, b.vendor,
                b.usage_family,
                uf_qty = SUM(b.usage_quantity)
            FROM base b
            WHERE b.usage_family IS NOT NULL
            GROUP BY b.billing_date, b.resource_id, b.vendor, b.usage_family
        ),
        uf_qty_json AS
        (
            SELECT
                billing_date, resource_id, vendor,
                usage_family_quantity =
                    '{' + STRING_AGG(
                            CONCAT('"', REPLACE(usage_family,'"','\"'), '":', CONVERT(VARCHAR(50), uf_qty)),
                            ','
                         ) + '}'
            FROM uf_qty_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        -------------------------------------------------------------------------
        -- 8) usage_types: distinct list (not numeric) – keep it compact
        -------------------------------------------------------------------------
        usage_types_list AS
        (
            SELECT
                billing_date, resource_id, vendor,
                usage_types =
                    STRING_AGG(usage_type, ',')
            FROM
            (
                SELECT DISTINCT
                    b.billing_date, b.resource_id, b.vendor, b.usage_type
                FROM base b
                WHERE b.usage_type IS NOT NULL
            ) d
            GROUP BY billing_date, resource_id, vendor
        ),

        -------------------------------------------------------------------------
        -- 9) Final staging row = core + the 5 string columns
        -------------------------------------------------------------------------
        final AS
        (
            SELECT
                c.billing_date,
                c.resource_id,
                c.vendor_account_name,
                c.vendor,

                -- totals
                c.overall_amortized_spend,
                c.overall_usage_quantity,

                -- “big string” columns (CAST to match your table types; change if needed)
                operation_cost        = CAST(oc.operation_cost        AS VARCHAR(4000)),
                operation_usage       = CAST(ou.operation_usage       AS VARCHAR(4000)),
                usage_family_cost     = CAST(ufc.usage_family_cost    AS VARCHAR(4000)),
                usage_family_quantity = CAST(ufq.usage_family_quantity AS VARCHAR(4000)),
                usage_types           = CAST(ut.usage_types           AS VARCHAR(4000)),

                c.azure_resource_name,
                c.azure_resource_group,
                c.service_name,
                c.vendor_account_identifier,
                c.region,
                c.humana_application_id,
                c.humana_resource_id,
                c.updated_date,
                c.last_modified_date
            FROM core c
            LEFT JOIN op_cost_json     oc  ON oc.billing_date=c.billing_date AND oc.resource_id=c.resource_id AND oc.vendor=c.vendor
            LEFT JOIN op_usage_json    ou  ON ou.billing_date=c.billing_date AND ou.resource_id=c.resource_id AND ou.vendor=c.vendor
            LEFT JOIN uf_cost_json     ufc ON ufc.billing_date=c.billing_date AND ufc.resource_id=c.resource_id AND ufc.vendor=c.vendor
            LEFT JOIN uf_qty_json      ufq ON ufq.billing_date=c.billing_date AND ufq.resource_id=c.resource_id AND ufq.vendor=c.vendor
            LEFT JOIN usage_types_list ut  ON ut.billing_date=c.billing_date AND ut.resource_id=c.resource_id AND ut.vendor=c.vendor
        )

        -------------------------------------------------------------------------
        -- 10) Insert into staging
        -------------------------------------------------------------------------
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
            Humana_resource_id,
            updated_date,
            last_modified_date
        )
        SELECT
            billing_date,
            resource_id,
            vendor_account_name,
            vendor,
            overall_amortized_spend,
            ISNULL(operation_cost,'{}'),
            ISNULL(operation_usage,'{}'),
            overall_usage_quantity,
            azure_resource_name,
            azure_resource_group,
            service_name,
            ISNULL(usage_family_cost,'{}'),
            ISNULL(usage_family_quantity,'{}'),
            ISNULL(usage_types,''),
            vendor_account_identifier,
            region,
            humana_application_id,
            humana_resource_id,
            updated_date,
            last_modified_date
        FROM final;

        -------------------------------------------------------------------------
        -- 11) UPDATE main (match on billing_date + resource_id + vendor)
        -------------------------------------------------------------------------
        UPDATE tgt
           SET tgt.vendor_account_name       = src.vendor_account_name,
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
               tgt.humana_application_id     = src.humana_application_id,
               tgt.Humana_resource_id        = src.Humana_resource_id,
               tgt.updated_date              = src.updated_date,
               tgt.last_modified_date        = src.last_modified_date
        FROM [Silver].[Cloudability_Daily_Resource_Cost] tgt
        JOIN [Silver].[Cloudability_Daily_Resource_Cost_Staging] src
          ON tgt.billing_date = src.billing_date
         AND tgt.resource_id  = src.resource_id
         AND tgt.vendor       = src.vendor;

        -------------------------------------------------------------------------
        -- 12) INSERT new rows only (no deletes)
        -------------------------------------------------------------------------
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
            Humana_resource_id,
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
            src.humana_application_id,
            src.Humana_resource_id,
            src.updated_date,
            src.last_modified_date
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
        -- IMPORTANT: do NOT use THROW @ErrNum,... because Synapse/SQL requires 50000+
        -- This rethrows the original error safely.
        THROW;
    END CATCH
END
GO
