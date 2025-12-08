/* Create schema if it doesn't exist */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

/* Silver table: one row per resource per day */
CREATE TABLE silver.Cloudability_Daily_Resource_Cost
(
    -- Keys
    usage_date              DATE                NOT NULL,   -- cloudability.date
    resource_id             NVARCHAR(1024)      NOT NULL,   -- ARM resource ID

    -- Normalized / Humana IDs
    humana_resource_id      NVARCHAR(200)       NULL,
    humana_application_id   NVARCHAR(100)       NULL,

    -- Azure identity
    azure_resource_name     NVARCHAR(512)       NULL,
    azure_resource_group    NVARCHAR(256)       NULL,
    subscription_id         NVARCHAR(64)        NULL,
    vendor_account_name     NVARCHAR(256)       NULL,       -- e.g. az3-pharmacy-npe
    service_name            NVARCHAR(256)       NULL,       -- e.g. Microsoft.DBforPostgreSQL
    resource_type           NVARCHAR(128)       NULL,       -- DB, VM, Disk, VNet, etc.
    region                  NVARCHAR(100)       NULL,       -- US Central, etc.
    environment             NVARCHAR(50)        NULL,       -- SBX / NPE / DEV / PROD

    -- Tag-based ownership
    owner_tag               NVARCHAR(256)       NULL,       -- owner/email tag
    cost_center             NVARCHAR(64)        NULL,       -- cost center / finance tag

    -- Cost metrics
    total_cost              DECIMAL(18,6)       NOT NULL,   -- SUM(amortized_spend)
    cost_breakdown_json     NVARCHAR(MAX)       NULL,       -- per operation: Storage, vCore, Backup...
    usage_summary_json      NVARCHAR(MAX)       NULL,       -- optional: vcore_hours, storage_gb, etc.

    -- Status
    resource_status         VARCHAR(20)         NULL,       -- Active / Deleted / Orphaned

    -- Audit
    inserted_at             DATETIME2(3)        NOT NULL 
        CONSTRAINT DF_CloudDailyCost_inserted_at DEFAULT SYSUTCDATETIME(),
    updated_at              DATETIME2(3)        NULL,

    CONSTRAINT PK_Cloudability_Daily_Resource_Cost 
        PRIMARY KEY CLUSTERED (usage_date, resource_id)
);
GO

/* Helpful indexes for queries */
CREATE NONCLUSTERED INDEX IX_CloudDailyCost_AppEnv
    ON silver.Cloudability_Daily_Resource_Cost (humana_application_id, environment, usage_date);
GO

CREATE NONCLUSTERED INDEX IX_CloudDailyCost_Subscription
    ON silver.Cloudability_Daily_Resource_Cost (subscription_id, usage_date);
GO

/* Ensure JSON columns are valid JSON (SQL Server 2016+) */
ALTER TABLE silver.Cloudability_Daily_Resource_Cost
ADD CONSTRAINT CK_CloudDailyCost_CostBreakdown_IsJson
    CHECK (cost_breakdown_json IS NULL OR ISJSON(cost_breakdown_json) = 1),
    CONSTRAINT CK_CloudDailyCost_UsageSummary_IsJson
    CHECK (usage_summary_json IS NULL OR ISJSON(usage_summary_json) = 1);
GO
/* ==========================================================
   STEP 1: Base aggregation per date/resource/operation
   ========================================================== */

WITH base AS
(
    SELECT
          d.[date]                                AS usage_date           -- cost date
        , d.resource_id                           AS resource_id
        , d.humana_resource_id                    AS humana_resource_id   -- adjust if name differs
        , d.humana_application_id                 AS humana_application_id
        , d.azure_resource_name                   AS azure_resource_name  -- adjust if name differs
        , d.azure_resource_group                  AS azure_resource_group -- adjust if name differs
        , d.vendor_account_name                   AS vendor_account_name  -- e.g. az3-pharmacy-npe
        , d.subscription_id                       AS subscription_id      -- if you have explicit column
        , d.service_name                          AS service_name         -- Microsoft.DBforPostgreSQL
        , d.usage_family                          AS usage_family         -- Storage, Compute, Security…
        , d.region                                AS region               -- US Central, etc.
        , d.operation                             AS operation            -- Storage Data Stored, vCore, Security...
        , SUM(d.amortized_spend)  AS operation_cost                      -- cost for this op
        , SUM(d.usage_quantity)   AS operation_usage                     -- usage for this op
        , MAX(d.environment_tag)  AS environment                         -- <- adjust/tag parsing
        , MAX(d.owner_tag)        AS owner_tag                           -- <- adjust/tag parsing
        , MAX(d.cost_center_tag)  AS cost_center                         -- <- adjust/tag parsing
    FROM cloudability.daily_spend d
    WHERE d.vendor = 'Azure'
      -- AND d.azure_resource_name = 'path-hpd-pgsql-flex-uat-cross-region-replica'  -- optional filter for testing
    GROUP BY
          d.[date]
        , d.resource_id
        , d.humana_resource_id
        , d.humana_application_id
        , d.azure_resource_name
        , d.azure_resource_group
        , d.vendor_account_name
        , d.subscription_id
        , d.service_name
        , d.usage_family
        , d.region
        , d.operation
),

/* ==========================================================
   STEP 2: Aggregate again per date/resource to build JSON
   ========================================================== */

agg AS
(
    SELECT
          b.usage_date
        , b.resource_id
        , b.humana_resource_id
        , b.humana_application_id
        , b.azure_resource_name
        , b.azure_resource_group
        , b.vendor_account_name
        , b.subscription_id
        , b.service_name
        , /* Pick one usage_family for the row (most common) */
          MAX(b.usage_family)              AS resource_type
        , b.region
        , MAX(b.environment)              AS environment
        , MAX(b.owner_tag)               AS owner_tag
        , MAX(b.cost_center)             AS cost_center

        , SUM(b.operation_cost)          AS total_cost

        /* JSON of cost per operation */
        , (
            SELECT
                  b2.operation           AS cost_type
                , b2.operation_cost      AS cost
            FROM base b2
            WHERE b2.usage_date = b.usage_date
              AND b2.resource_id = b.resource_id
            FOR JSON PATH
          ) AS cost_breakdown_json

        /* JSON of usage per operation (optional) */
        , (
            SELECT
                  b3.operation           AS usage_type
                , b3.operation_usage     AS usage_quantity
            FROM base b3
            WHERE b3.usage_date = b.usage_date
              AND b3.resource_id = b.resource_id
              AND b3.operation_usage IS NOT NULL
            FOR JSON PATH
          ) AS usage_summary_json
    FROM base b
    GROUP BY
          b.usage_date
        , b.resource_id
        , b.humana_resource_id
        , b.humana_application_id
        , b.azure_resource_name
        , b.azure_resource_group
        , b.vendor_account_name
        , b.subscription_id
        , b.service_name
        , b.region
)

/* ==========================================================
   STEP 3: Insert into Silver table
   ========================================================== */

INSERT INTO silver.Cloudability_Daily_Resource_Cost
(
      usage_date
    , resource_id
    , humana_resource_id
    , humana_application_id
    , azure_resource_name
    , azure_resource_group
    , subscription_id
    , vendor_account_name
    , service_name
    , resource_type
    , region
    , environment
    , owner_tag
    , cost_center
    , total_cost
    , cost_breakdown_json
    , usage_summary_json
    , resource_status
)
SELECT
      a.usage_date
    , a.resource_id
    , a.humana_resource_id
    , a.humana_application_id
    , a.azure_resource_name
    , a.azure_resource_group
    , a.subscription_id
    , a.vendor_account_name
    , a.service_name
    , a.resource_type
    , a.region
    , a.environment
    , a.owner_tag
    , a.cost_center
    , a.total_cost
    , a.cost_breakdown_json
    , a.usage_summary_json
    , 'Active'           AS resource_status   -- you can later derive Active/Deleted
FROM agg a;
GO


CREATE TABLE [Cloudability].[Daily_Spend_Aggregated]
(
    usage_date                DATE                               NULL,      -- Rule 1
    resource_id               NVARCHAR(500)                       NULL,      -- Rule 2
    vendor_account_name       NVARCHAR(250)                       NULL,      -- Rule 3
    vendor                    NVARCHAR(250)                       NULL,      -- Rule 4

    overall_amortized_spend   FLOAT                               NULL,      -- Rule 5

    itemized_cost             NVARCHAR(MAX)                       NULL,      -- Rule 6 (JSON)

    operations                NVARCHAR(MAX)                       NULL,      -- Rule 7 (array)

    overall_usage             NVARCHAR(MAX)                       NULL,      -- Rule 8 (JSON)

    overall_usgae_quantity    FLOAT                               NULL,      -- Rule 9 (sum of usage_quantity)

    azure_resource_name       NVARCHAR(500)                       NULL,      -- Rule 10
    azure_resource_group      NVARCHAR(500)                       NULL,      -- Rule 11
    service_name              NVARCHAR(500)                       NULL,      -- Rule 12

    usage_families            NVARCHAR(MAX)                       NULL,      -- Rule 13 (array)

    usgae_types               NVARCHAR(MAX)                       NULL,      -- Rule 14 (array)

    vendor_account_identifier NVARCHAR(250)                       NULL,      -- Rule 15
    region                    NVARCHAR(250)                       NULL,      -- Rule 16
    humana_application_id     NVARCHAR(250)                       NULL,      -- Rule 17
    Humana_resource_id        NVARCHAR(250)                       NULL,      -- Rule 18

    updated_date              DATETIME                            NULL,      -- Rule 19 (latest updated_date)
    last_modified_date       DATE                                NULL       -- Rule 19 (current date)
);
GO


REATE OR ALTER PROCEDURE [Cloudability].[usp_Aggregate_Daily_Spend]
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












START: Start ETL Workflow

    → PROCESS: Load configuration & constants
        Details: DB connection strings, resource type maps, invalid IDs

    → SUBPROCESS: load_sources() – Load data from SQL sources
        → DATABASE: Query Lenticular – [silver].[vw_ActiveResources]
        → DATABASE: Query Lenticular – [Gold].[ITag_Azure_InferredTags]
        → DATABASE: Query Hybrid ESA – [Silver].[SnowNormalized]
        → DATA: resources_df, virtual_tags_df, snow_df loaded

    → SUBPROCESS: transform(resources_df, virtual_tags_df, snow_df)
        → PROCESS: Normalize keys (resource_id_key, appsvc_key, eapm_key)

        → DECISION: IsOrphaned == 1 ?

            → NO (Non-Orphan Path):
                → PROCESS: Determine primary_appservice 
                    Details: From BillingOwnerAppsvcId / SupportOwnerAppsvcId
                → DATABASE: Left Join non-orphan with virtual_tags on resource_id_key
                → PROCESS: Compute final_app_service_id (primary vs tags)
                → DATABASE: Left Join non-orphan with SNOW on appsvc_key
                → PROCESS: Set ownership_path 
                    Options: non_orphan_appsvc_snow / non_orphan_tags_snow / non_orphan_tags_only

            → YES (Orphan Path):
                → DATABASE: Left Join orphan resources with virtual_tags
                → PROCESS: Pick orphan final_app_service_id 
                    Logic: EAPM → tag app_service_id → fallback patterns
                → DATABASE: Left Join orphan with SNOW on appsvc_key
                → PROCESS: Infer ownership_path 
                    Options: EAPM direct / tag-based / no match

        → PROCESS: Combine orphan and non-orphan results into final_df

        → PROCESS: Derive App Metadata
            - final_app_id
            - final_app_name
            - inferred_app_name
            - tag-based app patterns

        → PROCESS: Determine Billing + Support Owner
            - billing_owner_name/email
            - support_owner_name/email
            - fallback rules: SNOW → tags → EAPM → Resource metadata

        → PROCESS: Lookup Business Unit & Department
            From SNOW / EAPM maps

        → PROCESS: Compute platform team name
            If IsPlatformManaged == TRUE → support_owner_name

        → PROCESS: Compute Ownership Metadata
            Fields:
                ownership_determination_method
                ownership_confidence_score
                final_orphan flag
                orphan_reason
            Logic cases:
                - Direct EAPMID match (confidence 100)
                - Virtual Tagging Naming Pattern (40)
                - Tag app ID (60)
                - ParentTags (80)
                - NoTag (0)

        → PROCESS: Compute hash_key
            SHA256 of core business fields

        → PROCESS: Add timestamps & metadata
            - mapping_created_date
            - last_modified_date
            - is_current = TRUE
            - audit_id (GUID)

        → PROCESS: Standardize ResourceType
            Using resource_type_map

        → PROCESS: Rename & select final output columns

    → DECISION: Is gold_df empty or None?
        → YES → END: Transformation failed – Stop ETL
        → NO → Continue

    → PROCESS: Prepare load step
        - Build INSERT SQL
        - Convert dataframe to row lists
        - Split into batches (default batch size 1000)

    → SUBPROCESS: insert_gold_parallel(gold_df)
        → PROCESS: Truncate staging table
        → PROCESS: Create DB connection factory

        → SUBPROCESS: Parallel Insert (ThreadPoolExecutor)
            → For each batch:
                → PROCESS: Open new DB connection
                → PROCESS: Insert rows into staging table
            → PROCESS: Wait for all threads to complete
            → PROCESS: All batches processed successfully

        → PROCESS: Execute stored procedure
            Name: [Gold].[usp_AzureResourceNormalized]
            Purpose: Merge staging → Gold final table

END: ETL Workflow Complete (Gold table updated)









                 ┌───────────────────────────┐
                 │       START ETL          │
                 └───────────────────────────┘
                              │
                              ▼
                 ┌───────────────────────────┐
                 │ Load config & constants   │
                 │ (DB strings, maps, etc.) │
                 └───────────────────────────┘
                              │
                              ▼
                 ┌───────────────────────────┐
                 │       load_sources()      │
                 └───────────────────────────┘
                              │
     ┌────────────────────────┼────────────────────────┐
     ▼                        ▼                        ▼
┌───────────────┐      ┌───────────────┐       ┌──────────────────┐
│ Lenticular    │      │ Lenticular    │       │ Hybrid ESA       │
│ ActiveResources│      │ InferredTags │       │ SnowNormalized   │
└───────────────┘      └───────────────┘       └──────────────────┘
     │                        │                        │
     └─────────────┬──────────┴───────────┬────────────┘
                   ▼                      ▼
           ┌────────────────────────────────────┐
           │ resources_df, virtual_tags_df,     │
           │ snow_df are loaded into memory     │
           └────────────────────────────────────┘
                              │
                              ▼
                 ┌───────────────────────────┐
                 │   transform(...)          │
                 │   → build gold_df         │
                 └───────────────────────────┘
                              │
                              ▼
                 ┌───────────────────────────┐
                 │  Is gold_df empty/None?   │
                 └─────────────┬─────────────┘
                               │
                  YES          │          NO
                  ▼            │          ▼
        ┌────────────────┐     │   ┌─────────────────────────┐
        │ STOP: error    │     │   │ insert_gold_parallel()  │
        └────────────────┘     │   └─────────────────────────┘
                               │              │
                               │              ▼
                               │   ┌─────────────────────────┐
                               │   │ EXEC usp_AzureResource  │
                               │   │   Normalized (Gold SP)  │
                               │   └─────────────────────────┘
                               │              │
                               │              ▼
                               │   ┌─────────────────────────┐
                               └──▶│   END: ETL complete     │
                                   └─────────────────────────┘
           ┌───────────────────────────┐
           │     Start transform()     │
           └───────────────────────────┘
                          │
                          ▼
           ┌───────────────────────────┐
           │ Normalize keys            │
           │ (resource_id_key, etc.)   │
           └───────────────────────────┘
                          │
                          ▼
           ┌───────────────────────────┐
           │   IsOrphaned == 1 ?       │
           └───────────┬───────────────┘
                       │
          NON-ORPHAN   │   ORPHAN
          (0)          │   (1)
          ▼            │   ▼
┌────────────────┐     │   ┌─────────────────────────┐
│ NON-ORPHAN     │     │   │ ORPHAN PATH             │
│ PATH           │     │   └─────────────────────────┘
└────────────────┘     │
                       │
NON-ORPHAN PATH:                     ORPHAN PATH:
----------------                     -----------
1. Pick primary_appservice           1. Join orphan resources
   (Billing/Support appsvc)             with virtual_tags
2. Join with virtual_tags            2. Choose app ID
3. Compute final_app_service_id         (EAPM → tag → fallback)
4. Join with SNOW on appsvc_key      3. Join with SNOW on appsvc_key
5. Set ownership_path                4. Set ownership_path
   (appsvc_snow / tags_snow /           (EAPM direct / tags / none)
    tags_only)

          ▼                           ▼
          └─────────┬─────────────────┘
                    ▼
         ┌──────────────────────────────┐
         │   Combine both paths         │
         │   into final_df              │
         └──────────────────────────────┘
                    │
                    ▼
         ┌──────────────────────────────┐
         │ Derive app_id, app_name      │
         │ (from SNOW, tags, EAPM)      │
         └──────────────────────────────┘
                    │
                    ▼
         ┌──────────────────────────────┐
         │ Owners, BU, Dept,            │
         │ platform_team_name           │
         └──────────────────────────────┘
                    │
                    ▼
         ┌──────────────────────────────┐
         │ Ownership scoring            │
         │ method + confidence +        │
         │ final_orphan + reason        │
         └──────────────────────────────┘
                    │
                    ▼
         ┌──────────────────────────────┐
         │ hash_key, timestamps,        │
         │ audit_id, is_current         │
         └──────────────────────────────┘
                    │
                    ▼
         ┌──────────────────────────────┐
         │ Standardize resource type    │
         │ Select final columns         │
         └──────────────────────────────┘
                    │
                    ▼
         ┌──────────────────────────────┐
         │   Return gold_df             │
         └──────────────────────────────┘




           ┌───────────────────────────┐
           │  Start insert_gold_...    │
           └───────────────────────────┘
                          │
                          ▼
           ┌───────────────────────────┐
           │ Truncate staging table    │
           └───────────────────────────┘
                          │
                          ▼
           ┌───────────────────────────┐
           │ Build INSERT SQL          │
           │ Convert df → row list     │
           │ Split into batches        │
           └───────────────────────────┘
                          │
                          ▼
           ┌───────────────────────────┐
           │  For each batch (thread): │
           │  - open DB connection     │
           │  - insert rows            │
           └───────────────────────────┘
                          │
                          ▼
           ┌───────────────────────────┐
           │ Wait for all threads      │
           │ to finish                 │
           └───────────────────────────┘
                          │
                          ▼
           ┌───────────────────────────┐
           │ EXEC usp_AzureResource... │
           │ (merge staging → Gold)    │
           └───────────────────────────┘
                          │
                          ▼
           ┌───────────────────────────┐
           │    End insert_gold_...    │
           └───────────────────────────┘

