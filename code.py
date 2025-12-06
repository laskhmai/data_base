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