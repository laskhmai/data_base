-- HEAP with ROUND ROBIN distribution
CREATE TABLE [Silver].[Cloudability_Daily_Resource_Cost_GCP]
(
      billing_date                    DATE            NOT NULL
    , resource_id                     NVARCHAR(1000)  NOT NULL
    , vendor_account_name             NVARCHAR(500)
    , vendor                          NVARCHAR(50)    NOT NULL
    , overall_amortized_spend         DECIMAL(18,6)
    , operation_cost                  NVARCHAR(MAX)   -- JSON
    , operation_usage                 NVARCHAR(MAX)   -- JSON
    , overall_usage_quantity          DECIMAL(18,6)
    , gcp_resource_name               NVARCHAR(500)
    , gcp_project                     NVARCHAR(500)
    , service_name                    NVARCHAR(500)
    , usage_family_cost               NVARCHAR(MAX)   -- JSON
    , usage_family_quantity           NVARCHAR(MAX)   -- JSON
    , usage_types                     NVARCHAR(MAX)
    , vendor_account_identifier       NVARCHAR(500)
    , region                          NVARCHAR(200)
    , updated_date                    DATE
    , last_modified_date              DATE
    , reservation_identifier_cost     NVARCHAR(MAX)   -- JSON
    , humana_applicationid_cost       NVARCHAR(MAX)   -- JSON
    , humana_resourceid_cost          NVARCHAR(MAX)   -- JSON
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,  -- because NVARCHAR(MAX) exists
    HEAP                         -- cannot use CCI with NVARCHAR(MAX)
)
GO

-- Same for Staging Table
CREATE TABLE [Silver].[Cloudability_Daily_Resource_Cost_GCP_Staging]
(
      billing_date                    DATE
    , resource_id                     NVARCHAR(1000)
    , vendor_account_name             NVARCHAR(500)
    , vendor                          NVARCHAR(50)
    , overall_amortized_spend         DECIMAL(18,6)
    , operation_cost                  NVARCHAR(MAX)
    , operation_usage                 NVARCHAR(MAX)
    , overall_usage_quantity          DECIMAL(18,6)
    , gcp_resource_name               NVARCHAR(500)
    , gcp_project                     NVARCHAR(500)
    , service_name                    NVARCHAR(500)
    , usage_family_cost               NVARCHAR(MAX)
    , usage_family_quantity           NVARCHAR(MAX)
    , usage_types                     NVARCHAR(MAX)
    , vendor_account_identifier       NVARCHAR(500)
    , region                          NVARCHAR(200)
    , updated_date                    DATE
    , last_modified_date              DATE
    , reservation_identifier_cost     NVARCHAR(MAX)
    , humana_applicationid_cost       NVARCHAR(MAX)
    , humana_resourceid_cost          NVARCHAR(MAX)
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
)
GO