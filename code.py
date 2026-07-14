-- Step 1: Drop existing Normal recommendations table
IF OBJECT_ID('[Metrics].[MongoDBRightsizingRecommendations]')
   IS NOT NULL
DROP TABLE [Metrics].[MongoDBRightsizingRecommendations]
GO

-- Step 2: Recreate with all columns including autoscaling
CREATE TABLE [Metrics].[MongoDBRightsizingRecommendations]
(
    Month                    CHAR(7),
    ClusterKey               INT,
    ClusterName              NVARCHAR(255),
    OrgName                  NVARCHAR(255),
    ProjectKey               INT,
    ProviderName             NVARCHAR(255),
    RegionName               NVARCHAR(255),
    DayType                  NVARCHAR(50),
    HourType                 NVARCHAR(50),
    CurrentSku               NVARCHAR(100),
    CurrentCostPrHour        FLOAT,
    CpuRec                   NVARCHAR(100),
    MemRec                   NVARCHAR(100),
    ConnRec                  NVARCHAR(100),
    CpuAvgP95                FLOAT,
    CpuMaxP95                FLOAT,
    PeakCpuMax               FLOAT,
    MemUtilizationPct        FLOAT,
    ConnUtilizationPct       FLOAT,
    RecommendedSku           NVARCHAR(255),
    RecommendedCostPrHour    FLOAT,
    EstimatedMonthlySavings  FLOAT,
    Comment                  NVARCHAR(500),
    CurrentEfficiency        NVARCHAR(MAX),
    WithinEfficiency         NVARCHAR(MAX),
    LowCpuEfficiency         NVARCHAR(MAX),
    Spend30days              FLOAT,
    WithinFamilySavings      FLOAT,
    LowCpuSku                NVARCHAR(100),
    LowCpuSavings            FLOAT,
    Action                   NVARCHAR(50),
    -- Auto-scaling columns
    AutoScaleEnabled         BIT           NULL,
    ScaleDownEnabled         BIT           NULL,
    MinInstanceSize          NVARCHAR(20)  NULL,
    MaxInstanceSize          NVARCHAR(20)  NULL,
    RecommendedMinSku        NVARCHAR(20)  NULL,
    RecommendedMaxSku        NVARCHAR(20)  NULL,
    SavingsBasis             NVARCHAR(30)  NULL,
    AuditUtc                 DATETIME
)
GO

-- Step 3: Verify columns created correctly
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'Metrics'
AND   TABLE_NAME   = 'MongoDBRightsizingRecommendations'
ORDER BY ORDINAL_POSITION
GO