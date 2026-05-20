CREATE TABLE [Metrics].[MongoDBRightsizingRecommendations]
(
    -- IDENTITY
    Month                           VARCHAR(10)     NULL,
    ClusterKey                      INT             NULL,
    ClusterName                     VARCHAR(255)    NULL,
    OrgName                         VARCHAR(255)    NULL,
    ProjectKey                      INT             NULL,
    ProviderName                    VARCHAR(20)     NULL,
    RegionName                      VARCHAR(50)     NULL,

    -- CURRENT SKU
    CurrentSku                      VARCHAR(100)    NULL,
    CurrentCostPrHour               FLOAT           NULL,

    -- RECOMMENDATIONS
    CpuRec                          VARCHAR(20)     NULL,
    MemRec                          VARCHAR(20)     NULL,
    ConnRec                         VARCHAR(20)     NULL,

    -- KEY METRICS
    AvgCpuMax                       FLOAT           NULL,
    PeakCpuMax                      FLOAT           NULL,
    MemUtilizationPct               FLOAT           NULL,
    ConnUtilizationPct              FLOAT           NULL,

    -- RECOMMENDED SKU
    RecommendedSku                  VARCHAR(100)    NULL,
    RecommendedCostPrHour           FLOAT           NULL,
    EstimatedMonthlySavings         FLOAT           NULL,

    -- COMMENTS
    Comment                         VARCHAR(500)    NULL,
    MiscComment                     VARCHAR(500)    NULL,

    -- EFFICIENCY
    CurrentEfficiency               VARCHAR(MAX)    NULL,
    WithinEfficiency                VARCHAR(MAX)    NULL,
    OutsideEfficiency               VARCHAR(MAX)    NULL,

    -- SAVINGS
    Spend30days                     FLOAT           NULL,
    WithinFamilySavings             FLOAT           NULL,
    OverallDifferentVersionSavings  FLOAT           NULL,

    -- FINAL ACTION
    Action                          NVARCHAR(20)    NULL,

    -- ✅ AUDIT — When row was inserted
    AuditUtc                        DATETIME2(3)    NULL

)
GO

-- Verify
SELECT
    ORDINAL_POSITION    AS [#],
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'Metrics'
AND   TABLE_NAME   = 'MongoDBRightsizingRecommendations'
ORDER BY ORDINAL_POSITION
GO