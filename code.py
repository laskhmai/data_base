SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [Silver].[usp_CloudabilityAggregate_DailySpend_GCP] AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @UsageDate DATE = DATEADD(DAY, -3, CAST(GETDATE() AS DATE));

    BEGIN TRY

        /* Step 1: Clean staging table */
        TRUNCATE TABLE [Silver].[Cloudability_Daily_Resource_Cost_GCP_Staging];

        WITH base AS
        (
            SELECT
                  billing_date              = CONVERT(DATE, s.[date])
                , resource_id               = s.resource_id
                , vendor_account_name       = s.vendor_account_name
                , vendor                    = s.vendor
                , gcp_resource_name         = s.Azure_Resource_Name
                , gcp_project               = s.vendor_account_name
                , service_name              = s.service_name
                , vendor_account_identifier = s.vendor_account_identifier
                , region                    = s.region
                , humana_application_id     = s.Humana_Application_ID
                , humana_resource_id        = s.[Humana_Resource_ID(tag23)]
                -- FIXED: ROUND to avoid scientific notation
                , amortized_spend           = ROUND(ISNULL(CONVERT(DECIMAL(38,6), s.amortized_spend), 0.0), 6)
                , usage_quantity            = ROUND(ISNULL(CONVERT(DECIMAL(38,10), s.usage_quantity), 0.0), 10)
                , [Operation]               = s.[Operation]
                , usage_family              = s.usage_family
                , reservation_identifier    = s.reservation_identifier
                , usage_type                = s.usage_type
                , updated_date              = CONVERT(DATE, s.updated_date)
            FROM [Cloudability].[Daily_Spend] s
            WHERE s.vendor = 'GCP'
              AND CONVERT(DATE, s.[date]) = @UsageDate
        ),

        /* Step 2: ONE row per resource/date/vendor */
        parent AS
        (
            SELECT
                  b.billing_date
                , b.resource_id
                , b.vendor
                , vendor_account_name       = MAX(b.vendor_account_name)
                , overall_amortized_spend   = SUM(b.amortized_spend)
                , overall_usage_quantity    = SUM(b.usage_quantity)
                , gcp_resource_name         = MAX(b.gcp_resource_name)
                , gcp_project               = MAX(b.gcp_project)
                , service_name              = MAX(b.service_name)
                , usage_types               = NULL
                , vendor_account_identifier = MAX(b.vendor_account_identifier)
                , region                    = MAX(b.region)
                , humana_application_id     = MAX(b.humana_application_id)
                , humana_resource_id        = MAX(b.humana_resource_id)
                , updated_date              = MAX(b.updated_date)
                , last_modified_date        = CONVERT(DATE, GETDATE())
            FROM base b
            GROUP BY
                  b.billing_date
                , b.resource_id
                , b.vendor
        ),

        /* Step 3: OPERATION COST JSON */
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
                        -- FIXED: CONVERT to DECIMAL to remove E notation
                        CONCAT('"', [Operation], '":', 
                               CONVERT(VARCHAR(50), CONVERT(DECIMAL(38,6), op_spend))),
                        ','
                    ) + '}'
            FROM op_cost_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* Step 4: OPERATION USAGE JSON */
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
                        CONCAT('"', [Operation], '":', 
                               CONVERT(VARCHAR(50), CONVERT(DECIMAL(38,10), op_qty))),
                        ','
                    ) + '}'
            FROM op_usage_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* Step 5: USAGE FAMILY COST JSON */
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
                        CONCAT('"', usage_family, '":', 
                               CONVERT(VARCHAR(50), CONVERT(DECIMAL(38,6), fam_spend))),
                        ','
                    ) + '}'
            FROM fam_cost_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* Step 6: USAGE FAMILY QUANTITY JSON */
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
                        CONCAT('"', usage_family, '":', 
                               CONVERT(VARCHAR(50), CONVERT(DECIMAL(38,10), fam_qty))),
                        ','
                    ) + '}'
            FROM fam_qty_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* Step 7: USAGE TYPES */
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
        ),

        /* Step 8: RESERVATION IDENTIFIER COST JSON */
        ri_sum AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , ri_key   = COALESCE(reservation_identifier, 'NULL')
                , ri_spend = SUM(amortized_spend)
            FROM base
            GROUP BY billing_date, resource_id, vendor
                   , COALESCE(reservation_identifier, 'NULL')
        ),
        ri_rollup AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , reservation_identifier_cost =
                    '{' + STRING_AGG(
                        CAST(CONCAT('"', ri_key, '":', 
                             CONVERT(VARCHAR(50), CONVERT(DECIMAL(38,6), ri_spend)))
                        AS nvarchar(max)),
                        ','
                    ) + '}'
            FROM ri_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* Step 9: HUMANA APPLICATION COST JSON */
        app_sum AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , app_key   = COALESCE(humana_application_id, 'NULL')
                , app_spend = SUM(amortized_spend)
            FROM base
            GROUP BY billing_date, resource_id, vendor
                   , COALESCE(humana_application_id, 'NULL')
        ),
        app_rollup AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , humana_applicationid_cost =
                    '{' + STRING_AGG(
                        CAST(CONCAT('"', app_key, '":', 
                             CONVERT(VARCHAR(50), CONVERT(DECIMAL(38,6), app_spend)))
                        AS nvarchar(max)),
                        ','
                    ) + '}'
            FROM app_sum
            GROUP BY billing_date, resource_id, vendor
        ),

        /* Step 10: HUMANA RESOURCE COST JSON */
        hr_sum AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , hr_key   = COALESCE(humana_resource_id, 'NULL')
                , hr_spend = SUM(amortized_spend)
            FROM base
            GROUP BY billing_date, resource_id, vendor
                   , COALESCE(humana_resource_id, 'NULL')
        ),
        hr_rollup AS
        (
            SELECT
                  billing_date, resource_id, vendor
                , humana_resourceid_cost =
                    '{' + STRING_AGG(
                        CAST(CONCAT('"', hr_key, '":', 
                             CONVERT(VARCHAR(50), CONVERT(DECIMAL(38,6), hr_spend)))
                        AS nvarchar(max)),
                        ','
                    ) + '}'
            FROM hr_sum
            GROUP BY billing_date, resource_id, vendor
        )

        /* Step 11: INSERT into staging */
        INSERT INTO [Silver].[Cloudability_Daily_Resource_Cost_GCP_Staging]
        (
              billing_date
            , resource_id
            , vendor_account_name
            , vendor
            , overall_amortized_spend
            , operation_cost
            , operation_usage
            , overall_usage_quantity
            , gcp_resource_name
            , gcp_project
            , service_name
            , usage_family_cost
            , usage_family_quantity
            , usage_types
            , vendor_account_identifier
            , region
            , updated_date
            , last_modified_date
            , reservation_identifier_cost
            , humana_applicationid_cost
            , humana_resourceid_cost
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
            , p.gcp_resource_name
            , p.gcp_project
            , p.service_name
            , fc.usage_family_cost
            , fq.usage_family_quantity
            , ut.usage_types
            , p.vendor_account_identifier
            , p.region
            , p.updated_date
            , p.last_modified_date
            , ri.reservation_identifier_cost
            , ar.humana_applicationid_cost
            , hr.humana_resourceid_cost
        FROM parent p
        LEFT JOIN op_cost_json    oc ON oc.billing_date = p.billing_date
                                    AND oc.resource_id  = p.resource_id
                                    AND oc.vendor       = p.vendor
        LEFT JOIN op_usage_json   ou ON ou.billing_date = p.billing_date
                                    AND ou.resource_id  = p.resource_id
                                    AND ou.vendor       = p.vendor
        LEFT JOIN fam_cost_json   fc ON fc.billing_date = p.billing_date
                                    AND fc.resource_id  = p.resource_id
                                    AND fc.vendor       = p.vendor
        LEFT JOIN fam_qty_json    fq ON fq.billing_date = p.billing_date
                                    AND fq.resource_id  = p.resource_id
                                    AND fq.vendor       = p.vendor
        LEFT JOIN usage_types_agg ut ON ut.billing_date = p.billing_date
                                    AND ut.resource_id  = p.resource_id
                                    AND ut.vendor       = p.vendor
        LEFT JOIN ri_rollup       ri ON ri.billing_date = p.billing_date
                                    AND ri.resource_id  = p.resource_id
                                    AND ri.vendor       = p.vendor
        LEFT JOIN app_rollup      ar ON ar.billing_date = p.billing_date
                                    AND ar.resource_id  = p.resource_id
                                    AND ar.vendor       = p.vendor
        LEFT JOIN hr_rollup       hr ON hr.billing_date = p.billing_date
                                    AND hr.resource_id  = p.resource_id
                                    AND hr.vendor       = p.vendor
        ;

        /* Step 12: UPDATE existing rows in main table */
        UPDATE tgt
        SET
              tgt.vendor_account_name         = src.vendor_account_name
            , tgt.overall_amortized_spend     = src.overall_amortized_spend
            , tgt.operation_cost              = src.operation_cost
            , tgt.operation_usage             = src.operation_usage
            , tgt.overall_usage_quantity      = src.overall_usage_quantity
            , tgt.gcp_resource_name           = src.gcp_resource_name
            , tgt.gcp_project                 = src.gcp_project
            , tgt.service_name                = src.service_name
            , tgt.usage_family_cost           = src.usage_family_cost
            , tgt.usage_family_quantity       = src.usage_family_quantity
            , tgt.usage_types                 = src.usage_types
            , tgt.vendor_account_identifier   = src.vendor_account_identifier
            , tgt.region                      = src.region
            , tgt.updated_date                = src.updated_date
            , tgt.last_modified_date          = src.last_modified_date
            , tgt.reservation_identifier_cost = src.reservation_identifier_cost
            , tgt.humana_applicationid_cost   = src.humana_applicationid_cost
            , tgt.humana_resourceid_cost      = src.humana_resourceid_cost
        FROM [Silver].[Cloudability_Daily_Resource_Cost_GCP] tgt
        JOIN [Silver].[Cloudability_Daily_Resource_Cost_GCP_Staging] src
          ON tgt.billing_date = src.billing_date
         AND tgt.resource_id  = src.resource_id
         AND tgt.vendor       = src.vendor
        ;

        /* Step 13: INSERT new rows only */
        INSERT INTO [Silver].[Cloudability_Daily_Resource_Cost_GCP]
        (
              billing_date
            , resource_id
            , vendor_account_name
            , vendor
            , overall_amortized_spend
            , operation_cost
            , operation_usage
            , overall_usage_quantity
            , gcp_resource_name
            , gcp_project
            , service_name
            , usage_family_cost
            , usage_family_quantity
            , usage_types
            , vendor_account_identifier
            , region
            , updated_date
            , last_modified_date
            , reservation_identifier_cost
            , humana_applicationid_cost
            , humana_resourceid_cost
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
            , src.gcp_resource_name
            , src.gcp_project
            , src.service_name
            , src.usage_family_cost
            , src.usage_family_quantity
            , src.usage_types
            , src.vendor_account_identifier
            , src.region
            , src.updated_date
            , src.last_modified_date
            , src.reservation_identifier_cost
            , src.humana_applicationid_cost
            , src.humana_resourceid_cost
        FROM [Silver].[Cloudability_Daily_Resource_Cost_GCP_Staging] src
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM [Silver].[Cloudability_Daily_Resource_Cost_GCP] tgt
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