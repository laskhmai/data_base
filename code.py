/*  ============================================================
    Synapse / SQL DW note:
    - CREATE OR ALTER is NOT supported (PDW error).
    - Use DROP + CREATE (or run ALTER separately).
    - We also avoid THROW @ErrNum (because it can be < 50000).
      We use plain THROW; to rethrow the original error.
    ============================================================ */

IF OBJECT_ID('[Silver].[usp_CloudabilityAggregate_DailySpend]') IS NOT NULL
    DROP PROCEDURE [Silver].[usp_CloudabilityAggregate_DailySpend];
GO

CREATE PROCEDURE [Silver].[usp_CloudabilityAggregate_DailySpend]
(
      @ResolvedDate DATE = NULL   -- pass a specific billing date; if NULL we take GETDATE()-6
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @ResolvedDate IS NULL
        SET @ResolvedDate = DATEADD(DAY, -6, CAST(GETDATE() AS DATE));

    BEGIN TRY

        /* 1) Clear staging (only staging, NOT main table) */
        TRUNCATE TABLE [Silver].[Cloudability_Daily_Resource_Cost_Staging];

        /* 2) Base filtered rows for the day */
        WITH base AS
        (
            SELECT
                  billing_date = CONVERT(DATE, s.[date])
                , resource_id  =
                    STUFF(
                        s.resource_id,
                        1,
                        CASE WHEN CHARINDEX('/', s.resource_id) > 0 THEN CHARINDEX('/', s.resource_id) - 1 ELSE 0 END,
                        ''
                    )
                , s.vendor_account_name
                , s.vendor
                , azure_resource_name  = s.Azure_Resource_Name
                , azure_resource_group = s.[Azure_Resource_Group(tag11)]
                , s.service_name
                , s.vendor_account_identifier
                , s.region
                , humaa_application_id = s.Humana_Application_ID
                , humana_resource_id   = s.[Humana_Resource_ID(tag23)]
                , amortized_spend      = CAST(ISNULL(s.amortized_spend, 0.0) AS DECIMAL(18,8))
                , usage_quantity       = CAST(ISNULL(s.usage_quantity, 0.0) AS DECIMAL(18,8))
                , s.[Operation]
                , s.usage_family
                , s.usage_type
                , updated_date         = CONVERT(DATE, s.updated_date)
            FROM [Cloudability].[Daily_Spend] s
            WHERE s.vendor = 'Azure'
              AND CONVERT(DATE, s.[date]) = @ResolvedDate
        ),

        /* 3) One row per resource/day/vendor (the “parent” row) */
        parent AS
        (
            SELECT
                  b.billing_date
                , b.resource_id
                , b.vendor
                , vendor_account_name      = MAX(b.vendor_account_name)
                , overall_amortized_spend  = SUM(b.amortized_spend)
                , overall_usage_quantity   = SUM(b.usage_quantity)
                , azure_resource_name      = MAX(b.azure_resource_name)
                , azure_resource_group     = MAX(b.azure_resource_group)
                , service_name             = MAX(b.service_name)
                , usage_types              = NULL  -- filled later
                , vendor_account_identifier= MAX(b.vendor_account_identifier)
                , region                   = MAX(b.region)
                , humaa_application_id     = MAX(b.humaa_application_id)
                , humana_resource_id       = MAX(b.humana_resource_id)
                , updated_date             = MAX(b.updated_date)
                , last_modified_date       = CONVERT(DATE, GETDATE())
            FROM base b
            GROUP BY
                  b.billing_date
                , b.resource_id
                , b.vendor
        ),

        /* 4) OPERATION COST: SUM per operation, then STRING_AGG into ONE column */
        op_cost_sum AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , [Operation]
                , op_spend = SUM(amortized_spend)
            FROM base
            GROUP BY billing_date, resource_id, vendor, [Operation]
        ),
        op_cost_json AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , operation_cost =
                    '{' + STRING_AGG(
                            CONCAT('"', [Operation], '":', CONVERT(VARCHAR(50), op_spend)),
                            ','
                          ) + '}'
            FROM op_cost_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* 5) OPERATION USAGE: SUM per operation, then STRING_AGG into ONE column */
        op_usage_sum AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , [Operation]
                , op_qty = SUM(usage_quantity)
            FROM base
            GROUP BY billing_date, resource_id, vendor, [Operation]
        ),
        op_usage_json AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , operation_usage =
                    '{' + STRING_AGG(
                            CONCAT('"', [Operation], '":', CONVERT(VARCHAR(50), op_qty)),
                            ','
                          ) + '}'
            FROM op_usage_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* 6) USAGE FAMILY COST: SUM per usage_family, then STRING_AGG into ONE column */
        fam_cost_sum AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , usage_family
                , fam_spend = SUM(amortized_spend)
            FROM base
            GROUP BY billing_date, resource_id, vendor, usage_family
        ),
        fam_cost_json AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , usage_family_cost =
                    '{' + STRING_AGG(
                            CONCAT('"', usage_family, '":', CONVERT(VARCHAR(50), fam_spend)),
                            ','
                          ) + '}'
            FROM fam_cost_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* 7) USAGE FAMILY QUANTITY: SUM per usage_family, then STRING_AGG into ONE column */
        fam_qty_sum AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , usage_family
                , fam_qty = SUM(usage_quantity)
            FROM base
            GROUP BY billing_date, resource_id, vendor, usage_family
        ),
        fam_qty_json AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , usage_family_quantity =
                    '{' + STRING_AGG(
                            CONCAT('"', usage_family, '":', CONVERT(VARCHAR(50), fam_qty)),
                            ','
                          ) + '}'
            FROM fam_qty_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* 8) USAGE TYPES: distinct list (group first, then STRING_AGG) */
        usage_type_distinct AS
        (
            SELECT DISTINCT
                  billing_date, resource_id, vendor, usage_type
            FROM base
            WHERE usage_type IS NOT NULL
        ),
        usage_types_agg AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , usage_types = STRING_AGG(usage_type, ',')
            FROM usage_type_distinct
            GROUP BY billing_date, resource_id, vendor
        )

        /* 9) Insert ONE row per resource/day/vendor into staging */
        INSERT INTO [Silver].[Cloudability_Daily_Resource_Cost_Staging]
        (
              billing_date
            , resource_id
            , vendor_account_name
            , vendor
            , overall_amortized_spend
            , operation_cost
            , operation_usage
            , overall_usage_quantity
            , azure_resource_name
            , azure_resource_group
            , service_name
            , usage_family_cost
            , usage_family_quantity
            , usage_types
            , vendor_account_identifier
            , region
            , humaa_application_id
            , humana_resource_id
            , updated_date
            , last_modified_date
        )
        SELECT
              p.billing_date
            , p.resource_id
            , p.vendor_account_name
            , p.vendor
            , p.overall_amortized_spend
            , oc.operation_cost
            , ou.operation_usage
            , p.overall_usage_quantity
            , p.azure_resource_name
            , p.azure_resource_group
            , p.service_name
            , fc.usage_family_cost
            , fq.usage_family_quantity
            , ut.usage_types
            , p.vendor_account_identifier
            , p.region
            , p.humaa_application_id
            , p.humana_resource_id
            , p.updated_date
            , p.last_modified_date
        FROM parent p
        LEFT JOIN op_cost_json   oc ON oc.billing_date = p.billing_date AND oc.resource_id = p.resource_id AND oc.vendor = p.vendor
        LEFT JOIN op_usage_json  ou ON ou.billing_date = p.billing_date AND ou.resource_id = p.resource_id AND ou.vendor = p.vendor
        LEFT JOIN fam_cost_json  fc ON fc.billing_date = p.billing_date AND fc.resource_id = p.resource_id AND fc.vendor = p.vendor
        LEFT JOIN fam_qty_json   fq ON fq.billing_date = p.billing_date AND fq.resource_id = p.resource_id AND fq.vendor = p.vendor
        LEFT JOIN usage_types_agg ut ON ut.billing_date = p.billing_date AND ut.resource_id = p.resource_id AND ut.vendor = p.vendor
        ;

        /* 10) UPDATE existing rows in main table (match on billing_date + resource_id + vendor) */
        UPDATE tgt
        SET
              tgt.vendor_account_name       = src.vendor_account_name
            , tgt.overall_amortized_spend   = src.overall_amortized_spend
            , tgt.operation_cost            = src.operation_cost
            , tgt.operation_usage           = src.operation_usage
            , tgt.overall_usage_quantity    = src.overall_usage_quantity
            , tgt.azure_resource_name       = src.azure_resource_name
            , tgt.azure_resource_group      = src.azure_resource_group
            , tgt.service_name              = src.service_name
            , tgt.usage_family_cost         = src.usage_family_cost
            , tgt.usage_family_quantity     = src.usage_family_quantity
            , tgt.usage_types               = src.usage_types
            , tgt.vendor_account_identifier = src.vendor_account_identifier
            , tgt.region                    = src.region
            , tgt.humaa_application_id      = src.humaa_application_id
            , tgt.humana_resource_id        = src.humana_resource_id
            , tgt.updated_date              = src.updated_date
            , tgt.last_modified_date        = src.last_modified_date
        FROM [Silver].[Cloudability_Daily_Resource_Cost] tgt
        JOIN [Silver].[Cloudability_Daily_Resource_Cost_Staging] src
          ON  tgt.billing_date = src.billing_date
          AND tgt.resource_id  = src.resource_id
          AND tgt.vendor       = src.vendor;

        /* 11) INSERT new rows only */
        INSERT INTO [Silver].[Cloudability_Daily_Resource_Cost]
        (
              billing_date
            , resource_id
            , vendor_account_name
            , vendor
            , overall_amortized_spend
            , operation_cost
            , operation_usage
            , overall_usage_quantity
            , azure_resource_name
            , azure_resource_group
            , service_name
            , usage_family_cost
            , usage_family_quantity
            , usage_types
            , vendor_account_identifier
            , region
            , humaa_application_id
            , humana_resource_id
            , updated_date
            , last_modified_date
        )
        SELECT
              src.billing_date
            , src.resource_id
            , src.vendor_account_name
            , src.vendor
            , src.overall_amortized_spend
            , src.operation_cost
            , src.operation_usage
            , src.overall_usage_quantity
            , src.azure_resource_name
            , src.azure_resource_group
            , src.service_name
            , src.usage_family_cost
            , src.usage_family_quantity
            , src.usage_types
            , src.vendor_account_identifier
            , src.region
            , src.humaa_application_id
            , src.humana_resource_id
            , src.updated_date
            , src.last_modified_date
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
        /* Re-throw original error safely in Synapse/SQLDW */
        THROW;
    END CATCH
END
GO

/* Run */
-- EXEC [Silver].[usp_CloudabilityAggregate_DailySpend] '2025-12-19';
-- EXEC [Silver].[usp_CloudabilityAggregate_DailySpend];  -- default GETDATE()-6
