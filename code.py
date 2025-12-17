CREATE TABLE [Gold].[AzureGoldResourceNormalized]
(
    resource_id                     NVARCHAR(450)   NULL,
    resource_name                   NVARCHAR(500)   NULL,
    resource_type_standardized      NVARCHAR(200)   NULL,
    cloud_provider                  NVARCHAR(50)    NULL,
    cloud_account_id                NVARCHAR(200)   NULL,
    cloud_account_name              NVARCHAR(500)   NULL,
    region                          NVARCHAR(100)   NULL,
    environment                     NVARCHAR(50)    NULL,

    billing_owner_appsvcid          NVARCHAR(200)   NULL,
    support_owner_appsvcid          NVARCHAR(200)   NULL,
    billing_owner_appid             NVARCHAR(200)   NULL,
    support_owner_appid             NVARCHAR(200)   NULL,

    application_name                NVARCHAR(500)   NULL,

    billing_owner_email             NVARCHAR(500)   NULL,
    support_owner_email             NVARCHAR(500)   NULL,
    billing_owner_name              NVARCHAR(500)   NULL,
    support_owner_name              NVARCHAR(500)   NULL,

    business_unit                   NVARCHAR(200)   NULL,
    department                      NVARCHAR(200)   NULL,

    is_platform_managed             BIT             NULL,
    management_model                NVARCHAR(50)    NULL,
    platform_team_name              NVARCHAR(200)   NULL,

    ownership_confidence_score      INT             NULL,
    ownership_determination_method  NVARCHAR(200)   NULL,

    is_orphaned                     TINYINT         NULL,
    is_deleted                      BIT             NULL,
    orphan_reason                   NVARCHAR(200)   NULL,

    has_conflicting_tags            BIT             NULL,
    dependency_triggered_update     BIT             NULL,

    hash_key                        CHAR(64)        NULL,
    change_category                 NVARCHAR(100)   NULL,

    resource_created_date           DATETIME2(7)    NULL,
    mapping_created_date            DATETIME2(7)    NULL,
    last_verified_date              DATETIME2(7)    NULL,
    last_modified_date              DATETIME2(7)    NULL,

    is_current                      BIT             NULL,

    sourceHashKey                   NVARCHAR(500)   NULL
);



✅ Cloudability Cost Normalized Silver – Pseudo Code (Same Style)
1. Get date range to process

processing_dates = list of dates to load (ex: yesterday or last N days)

2. Loop each date

FOR each usage_date in processing_dates:

PRINT “Processing date: usage_date”

3. Load Cloudability raw rows for that date

raw_rows = SELECT rows from Cloudability_Daily_Spend
WHERE date = usage_date AND vendor = “Azure”

4. Basic cleanup / standardization

FOR each row in raw_rows:

make sure resource_id is not null

normalize resource_id format (trim, lowercase if required)

standardize operation name (remove extra spaces)

keep needed columns only (resource_id, operation, amortized_spend, tags, metadata)

5. Group to build “1 row per resource per day”

grouped_resources = GROUP raw_rows BY (usage_date, resource_id)

FOR each group (one resource for the day):

total_cost = SUM(amortized_spend for that resource/day)

6. Build cost breakdown for that resource/day

FOR each resource/day group:

cost_breakdown = GROUP rows BY operation

FOR each operation:

operation_cost = SUM(amortized_spend)

add to cost_breakdown list

store cost_breakdown into one column (JSON/text)

Example:

Storage → 3.04

vCore → 5.88

Security → 0.59

7. Build usage summary (optional)

FOR each resource/day group:

usage_breakdown = GROUP rows BY operation

FOR each operation:

operation_usage = SUM(usage_quantity)

add to usage_breakdown list

store usage_breakdown into one column (JSON/text)

8. Capture metadata for the Silver record

FOR each resource/day group:

Pick stable values (usually same in all rows):

vendor_account_name (subscription/billing account)

service_name

region

azure_resource_name

azure_resource_group

humana_application_id (if present)

humana_resource_id (if present)

environment, owner, cost_center (from tags)

9. Detect NEW vs CHANGED Silver records

previous_silver = existing record for (usage_date, resource_id)

IF no previous_silver:

mark as NEW
ELSE IF total_cost or cost_breakdown or tags changed:

mark as CHANGED
ELSE:

mark as UNCHANGED

IF UNCHANGED:

skip insert/update

10. Upsert into Silver table

IF NEW:

insert record into Silver_Cost_Normalized
IF CHANGED:

update existing record in Silver_Cost_Normalized

11. Audit tag changes (separate requirement)

Compare today tags vs last available day tags for same resource:

IF owner/app/cost_center/environment changed:

write a record into Tag_Audit table:

resource_id

tag_name

old_value

new_value

changed_date




CREATE OR ALTER PROCEDURE [Cloudability].[usp_Aggregate_Daily_Spend]
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------------------------------------------
    -- OPTIONAL: Clear target table before reload (uncomment if you want this)
    ----------------------------------------------------------------------------
    -- TRUNCATE TABLE [Cloudability].[Daily_Spend_Aggregated];

    ----------------------------------------------------------------------------
    -- Insert aggregated data
    ----------------------------------------------------------------------------
    INSERT INTO [Cloudability].[Daily_Spend_Aggregated]
    (
          usage_date
        , resource_id
        , vendor_account_name
        , vendor
        , overall_amortized_spend
        , itemized_cost
        , operations
        , overall_usage                -- JSON: operation -> usage_quantity
        , overall_usgae_quantity       -- Sum of usage_quantity
        , azure_resource_name
        , azure_resource_group
        , service_name
        , usage_families
        , usgae_types
        , vendor_account_identifier
        , region
        , humana_application_id
        , Humana_resource_id
        , updated_date
        , last_modified_name
    )
    SELECT
          s.[date]                       AS usage_date               -- 1
        , s.resource_id                  AS resource_id              -- 2
        , s.vendor_account_name          AS vendor_account_name      -- 3
        , s.Vendor                       AS vendor                   -- 4

        , SUM(ISNULL(s.amortized_spend, 0.0)) AS overall_amortized_spend    -- 5

        --------------------------------------------------------------------
        -- 6. JSON: { "operation1": amount1, "operation2": amount2, ... }
        --------------------------------------------------------------------
        , CONCAT(
              '{'
            , STRING_AGG(
                  CONCAT(
                      '"', s.Operation, '":',
                      COALESCE(CAST(s.amortized_spend AS NVARCHAR(50)), '0')
                  ),
                  ','
              )
            , '}'
          ) AS itemized_cost

        --------------------------------------------------------------------
        -- 7. Array of operations (comma-separated list)
        --------------------------------------------------------------------
        , STRING_AGG(s.Operation, ',') AS operations

        --------------------------------------------------------------------
        -- 8. JSON: { "operation1": usage_qty1, "operation2": usage_qty2, ... }
        --    Mapped into target column overall_usage
        --------------------------------------------------------------------
        , CONCAT(
              '{'
            , STRING_AGG(
                  CONCAT(
                      '"', s.Operation, '":',
                      COALESCE(CAST(s.usage_quantity AS NVARCHAR(50)), '0')
                  ),
                  ','
              )
            , '}'
          ) AS overall_usage

        --------------------------------------------------------------------
        -- 9. Sum of usage_quantity
        --------------------------------------------------------------------
        , SUM(ISNULL(s.usage_quantity, 0.0)) AS overall_usgae_quantity

        -- 10, 11, 12
        , s.azure_resource_name         AS azure_resource_name
        , s.azure_resource_group        AS azure_resource_group
        , s.service_name                AS service_name

        --------------------------------------------------------------------
        -- 13. Array of usage_family values (comma-separated)
        --------------------------------------------------------------------
        , STRING_AGG(s.usage_family, ',') AS usage_families

        --------------------------------------------------------------------
        -- 14. Array of usage_type values (comma-separated)
        --------------------------------------------------------------------
        , STRING_AGG(s.usage_type, ',')    AS usgae_types

        -- 15, 16, 17, 18
        , s.vendor_account_identifier   AS vendor_account_identifier
        , s.Region                      AS region
        , s.humana_application_id       AS humana_application_id
        , s.Humana_resource_id          AS Humana_resource_id

        --------------------------------------------------------------------
        -- updated_date: taking latest timestamp in the group
        --------------------------------------------------------------------
        , MAX(s.updated_date)           AS updated_date

        --------------------------------------------------------------------
        -- 19. last_modified_name = current date
        --------------------------------------------------------------------
        , CONVERT(date, GETDATE())      AS last_modified_date
    FROM
        [Cloudability].[Daily_Spend] s
    GROUP BY
          s.[date]                      -- usage_date
        , s.resource_id
        , s.vendor_account_name
        , s.Vendor
        , s.azure_resource_name
        , s.azure_resource_group
        , s.service_name
        , s.vendor_account_identifier
        , s.Region
        , s.humana_application_id
        , s.Humana_resource_id;
END;
GO