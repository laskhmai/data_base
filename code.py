DROP TABLE IF EXISTS [Metrics].[MongoDBRightsizingRecommendations]
GO

CREATE TABLE [Metrics].[MongoDBRightsizingRecommendations]
(
    Month                    NVARCHAR(7)    NULL,
    ClusterKey               INT            NULL,
    ClusterName              NVARCHAR(255)  NULL,
    OrgName                  NVARCHAR(255)  NULL,
    ProjectKey               INT            NULL,
    ProviderName             NVARCHAR(50)   NULL,
    RegionName               NVARCHAR(100)  NULL,
    DayType                  NVARCHAR(20)   NULL,
    HourType                 NVARCHAR(30)   NULL,
    CurrentSku               NVARCHAR(50)   NULL,
    CurrentCostPrHour        FLOAT          NULL,
    CpuRec                   NVARCHAR(50)   NULL,
    MemRec                   NVARCHAR(50)   NULL,
    ConnRec                  NVARCHAR(50)   NULL,
    AvgCpuMax                FLOAT          NULL,
    PeakCpuMax               FLOAT          NULL,
    MemUtilizationPct        FLOAT          NULL,
    ConnUtilizationPct       FLOAT          NULL,
    RecommendedSku           NVARCHAR(100)  NULL,
    RecommendedCostPrHour    FLOAT          NULL,
    EstimatedMonthlySavings  FLOAT          NULL,
    Comment                  NVARCHAR(500)  NULL,
    MiscComment              NVARCHAR(MAX)  NULL,
    CurrentEfficiency        NVARCHAR(MAX)  NULL,
    WithinEfficiency         NVARCHAR(MAX)  NULL,
    LowCpuEfficiency         NVARCHAR(MAX)  NULL,
    Spend30days              FLOAT          NULL,
    WithinFamilySavings      FLOAT          NULL,
    LowCpuSku                NVARCHAR(50)   NULL,
    LowCpuSavings            FLOAT          NULL,
    Action                   NVARCHAR(20)   NULL,
    AuditUtc                 DATETIME       NULL
)
WITH (HEAP)
GO