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
