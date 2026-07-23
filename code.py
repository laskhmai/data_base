-- Step 1: GCP Staging Table
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
GO

-- Step 2: GCP Main Table
CREATE TABLE [Silver].[Cloudability_Daily_Resource_Cost_GCP]
(
      billing_date                    DATE            NOT NULL
    , resource_id                     NVARCHAR(1000)  NOT NULL
    , vendor_account_name             NVARCHAR(500)
    , vendor                          NVARCHAR(50)    NOT NULL
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

    , CONSTRAINT PK_Cloudability_Daily_Resource_Cost_GCP
        PRIMARY KEY (billing_date, resource_id, vendor)
)
GO