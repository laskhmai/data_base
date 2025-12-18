IF OBJECT_ID('Silver.Cloudability_Daily_Resource_Cost_Staging', 'U') IS NULL
BEGIN
    CREATE TABLE Silver.Cloudability_Daily_Resource_Cost_Staging
    (
        usage_date                 date            NOT NULL,
        resource_id                nvarchar(500)   NOT NULL,
        vendor_account_name        nvarchar(250)   NOT NULL,
        vendor                     nvarchar(250)   NOT NULL,

        overall_amortized_spend    decimal(18,8)   NULL,
        itemized_cost              nvarchar(4000)  NULL,
        operations                 nvarchar(4000)  NULL,
        overall_usage              nvarchar(4000)  NULL,
        overall_usage_quantity     decimal(18,8)   NULL,

        azure_resource_name        nvarchar(500)   NULL,
        azure_resource_group       nvarchar(500)   NULL,
        service_name               nvarchar(500)   NULL,
        usage_families             nvarchar(2000)  NULL,
        usage_types                nvarchar(2000)  NULL,
        vendor_account_identifier  nvarchar(250)   NULL,
        region                     nvarchar(250)   NULL,

        humana_application_id      nvarchar(250)   NULL,
        humana_resource_id         nvarchar(250)   NULL,

        updated_date               datetime        NULL,
        last_modified_date         datetime        NULL
    );

    -- Helpful for MERGE speed (lightweight)
    CREATE INDEX IX_Cloudability_Daily_Resource_Cost_Stg_Key
    ON Silver.Cloudability_Daily_Resource_Cost_Staging (usage_date, vendor_account_name, vendor, resource_id);
END;
GO






IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_Cloudability_Daily_Resource_Cost_Report'
      AND object_id = OBJECT_ID('Silver.Cloudability_Daily_Resource_Cost')
)
BEGIN
    CREATE INDEX IX_Cloudability_Daily_Resource_Cost_Report
    ON Silver.Cloudability_Daily_Resource_Cost (vendor_account_name, usage_date)
    INCLUDE (overall_amortized_spend, overall_usage_quantity, azure_resource_name, service_name, region);
END;
GO




CREATE OR ALTER PROCEDURE Cloudability.usp_Load_Cloudability_Silver_Staging_Merge
(
    @StartDate date,                  -- inclusive
    @EndDate   date,                  -- exclusive (recommended)
    @Vendor    nvarchar(50) = 'Azure',
    @VendorAccountName nvarchar(250) = NULL,  -- optional: one subscription
    @DoMerge bit = 1                  -- 1 = merge into silver, 0 = only stage + validate
)
AS
BEGIN
    SET NOCOUNT ON;

    -------------------------------------------------------------------------
    -- 0) Clean staging for this run
    -------------------------------------------------------------------------
    TRUNCATE TABLE Silver.Cloudability_Daily_Resource_Cost_Staging;

    -------------------------------------------------------------------------
    -- 1) Load aggregated results into STAGING (DECIMAL + JSON-like strings)
    -------------------------------------------------------------------------
    INSERT INTO Silver.Cloudability_Daily_Resource_Cost_Staging
    (
        usage_date,
        resource_id,
        vendor_account_name,
        vendor,
        overall_amortized_spend,
        itemized_cost,
        operations,
        overall_usage,
        overall_usage_quantity,
        azure_resource_name,
        azure_resource_group,
        service_name,
        usage_families,
        usage_types,
        vendor_account_identifier,
        region,
        humana_application_id,
        humana_resource_id,
        updated_date,
        last_modified_date
    )
    SELECT
        CONVERT(date, s.[date]) AS usage_date,

        -- Normalize resource_id: remove leading slash so key is consistent
        STUFF(
            s.resource_id,
            1,
            CASE WHEN CHARINDEX('/', s.resource_id) > 0 THEN CHARINDEX('/', s.resource_id) - 1 ELSE 0 END,
            ''
        ) AS resource_id,

        s.vendor_account_name,
        s.vendor,

        SUM(CAST(ISNULL(s.amortized_spend, 0.0) AS decimal(18,8))) AS overall_amortized_spend,

        '{' + STRING_AGG(
                CONCAT(
                    '"', ISNULL(s.operation,''), '":',
                    COALESCE(CONVERT(varchar(50), CAST(ISNULL(s.amortized_spend,0.0) AS decimal(18,8))), '0')
                ),
            ','
        ) + '}' AS itemized_cost,

        STRING_AGG(ISNULL(s.operation,''), ',') AS operations,

        '{' + STRING_AGG(
                CONCAT(
                    '"', ISNULL(s.operation,''), '":',
                    COALESCE(CONVERT(varchar(50), CAST(ISNULL(s.usage_quantity,0.0) AS decimal(18,8))), '0')
                ),
            ','
        ) + '}' AS overall_usage,

        SUM(CAST(ISNULL(s.usage_quantity, 0.0) AS decimal(18,8))) AS overall_usage_quantity,

        MAX(s.azure_resource_name) AS azure_resource_name,
        MAX(s.[Azure_Resource_Group(tag11)]) AS azure_resource_group,
        MAX(s.service_name) AS service_name,

        STRING_AGG(ISNULL(s.usage_family,''), ',') AS usage_families,
        STRING_AGG(ISNULL(s.usage_type,''), ',')   AS usage_types,

        MAX(s.vendor_account_identifier) AS vendor_account_identifier,
        MAX(s.region) AS region,

        MAX(s.Humana_Application_ID) AS humana_application_id,
        MAX(s.[Humana_Resource_ID(tag23)]) AS humana_resource_id,

        MAX(TRY_CONVERT(datetime, s.updated_date)) AS updated_date,
        GETDATE() AS last_modified_date
    FROM cloudability.daily_spend s
    WHERE s.vendor = @Vendor
      AND CONVERT(date, s.[date]) >= @StartDate
      AND CONVERT(date, s.[date]) <  @EndDate
      AND (@VendorAccountName IS NULL OR s.vendor_account_name = @VendorAccountName)
    GROUP BY
        CONVERT(date, s.[date]),
        STUFF(
            s.resource_id,
            1,
            CASE WHEN CHARINDEX('/', s.resource_id) > 0 THEN CHARINDEX('/', s.resource_id) - 1 ELSE 0 END,
            ''
        ),
        s.vendor_account_name,
        s.vendor;

    -------------------------------------------------------------------------
    -- 2) Validation outputs (you can compare with base table totals)
    -------------------------------------------------------------------------
    DECLARE @StageRows bigint;
    DECLARE @StageCost decimal(38,8);
    DECLARE @StageUsage decimal(38,8);

    SELECT
        @StageRows = COUNT(*),
        @StageCost = SUM(overall_amortized_spend),
        @StageUsage = SUM(overall_usage_quantity)
    FROM Silver.Cloudability_Daily_Resource_Cost_Staging;

    SELECT
        @StageRows AS staging_row_count,
        @StageCost AS staging_total_cost,
        @StageUsage AS staging_total_usage_quantity;

    -------------------------------------------------------------------------
    -- 3) Merge into SILVER (Upsert)
    -------------------------------------------------------------------------
    IF (@DoMerge = 1)
    BEGIN
        BEGIN TRY
            BEGIN TRAN;

            MERGE Silver.Cloudability_Daily_Resource_Cost WITH (HOLDLOCK) AS tgt
            USING Silver.Cloudability_Daily_Resource_Cost_Staging AS src
            ON  tgt.usage_date = src.usage_date
            AND tgt.vendor_account_name = src.vendor_account_name
            AND tgt.vendor = src.vendor
            AND tgt.resource_id = src.resource_id

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.overall_amortized_spend   = src.overall_amortized_spend,
                    tgt.itemized_cost             = src.itemized_cost,
                    tgt.operations                = src.operations,
                    tgt.overall_usage             = src.overall_usage,
                    tgt.overall_usage_quantity    = src.overall_usage_quantity,
                    tgt.azure_resource_name       = src.azure_resource_name,
                    tgt.azure_resource_group      = src.azure_resource_group,
                    tgt.service_name              = src.service_name,
                    tgt.usage_families            = src.usage_families,
                    tgt.usage_types               = src.usage_types,
                    tgt.vendor_account_identifier = src.vendor_account_identifier,
                    tgt.region                    = src.region,
                    tgt.humana_application_id     = src.humana_application_id,
                    tgt.humana_resource_id        = src.humana_resource_id,
                    tgt.updated_date              = src.updated_date,
                    tgt.last_modified_date        = src.last_modified_date

            WHEN NOT MATCHED BY TARGET THEN
                INSERT
                (
                    usage_date, resource_id, vendor_account_name, vendor,
                    overall_amortized_spend, itemized_cost, operations, overall_usage, overall_usage_quantity,
                    azure_resource_name, azure_resource_group, service_name,
                    usage_families, usage_types, vendor_account_identifier, region,
                    humana_application_id, humana_resource_id,
                    updated_date, last_modified_date
                )
                VALUES
                (
                    src.usage_date, src.resource_id, src.vendor_account_name, src.vendor,
                    src.overall_amortized_spend, src.itemized_cost, src.operations, src.overall_usage, src.overall_usage_quantity,
                    src.azure_resource_name, src.azure_resource_group, src.service_name,
                    src.usage_families, src.usage_types, src.vendor_account_identifier, src.region,
                    src.humana_application_id, src.humana_resource_id,
                    src.updated_date, src.last_modified_date
                );

            COMMIT TRAN;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK TRAN;
            THROW;
        END CATCH;

        -- Cleanup staging (so next run is clean)
        TRUNCATE TABLE Silver.Cloudability_Daily_Resource_Cost_Staging;
    END
END;
GO
















