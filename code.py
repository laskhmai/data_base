-- Drop old STL table
IF OBJECT_ID('[Metrics].[MongoDBRightsizingRecommendations_STL]')
   IS NOT NULL
DROP TABLE [Metrics].[MongoDBRightsizingRecommendations_STL]
GO

-- Create fresh with correct columns
CREATE TABLE [Metrics].[MongoDBRightsizingRecommendations_STL]
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
    CpuAvgP95                FLOAT,         ← replaces AvgCpuMax
    CpuMaxP95                FLOAT,         ← new column
    PeakCpuMax               FLOAT,         ← keeps peak
    MemUtilizationPct        FLOAT,
    ConnUtilizationPct       FLOAT,
    RecommendedSku           NVARCHAR(255),
    RecommendedCostPrHour    FLOAT,
    EstimatedMonthlySavings  FLOAT,
    Comment                  NVARCHAR(500),
    MiscComment              NVARCHAR(500),
    CurrentEfficiency        NVARCHAR(MAX),
    WithinEfficiency         NVARCHAR(MAX),
    LowCpuEfficiency         NVARCHAR(MAX),
    Spend30days              FLOAT,
    WithinFamilySavings      FLOAT,
    LowCpuSku                NVARCHAR(100),
    LowCpuSavings            FLOAT,
    Action                   NVARCHAR(50),
    AuditUtc                 DATETIME
)
GO