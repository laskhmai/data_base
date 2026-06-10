-- Step 1: Rename columns
ALTER TABLE [Metrics].[MongoDBRightsizingRecommendations]
RENAME COLUMN OutsideEfficiency TO LowCpuEfficiency
GO
ALTER TABLE [Metrics].[MongoDBRightsizingRecommendations]
RENAME COLUMN OverallDifferentVersionSavings TO LowCpuSavings
GO

-- Step 2: Add new column
ALTER TABLE [Metrics].[MongoDBRightsizingRecommendations]
ADD LowCpuSku NVARCHAR(50) NULL
GO

-- Step 3: Create SimulatedMetrics table
CREATE TABLE [Metrics].[MongoDBRightsizingSimulatedMetrics]
( ... )  ← from earlier
GO

CREATE TABLE [Metrics].[MongoDBRightsizingSimulatedMetrics]
(
    ClusterKey          INT,
    ClusterName         NVARCHAR(255),
    CurrentSku          NVARCHAR(50),
    [Date]              DATE,
    [Hour]              INT,
    DayType             NVARCHAR(20),
    HourType            NVARCHAR(30),
    -- Raw current metrics
    CpuAvg              FLOAT,
    CpuMax              FLOAT,
    CpuAvgP95           FLOAT,
    CpuMaxP95           FLOAT,
    MemAvg              FLOAT,
    MemMax              FLOAT,
    MemAvgP95           FLOAT,
    MemMaxP95           FLOAT,
    ConnAvg             FLOAT,
    ConnMax             FLOAT,
    -- Recommendation SKUs
    RecommendedSku      NVARCHAR(100),
    LowCpuSku           NVARCHAR(50),
    -- Within family projections (Standard)
    nCpuAvgWithin       FLOAT,
    nCpuMaxWithin       FLOAT,
    nCpuAvgP95Within    FLOAT,
    nCpuMaxP95Within    FLOAT,
    nMemAvgWithin       FLOAT,
    nMemMaxWithin       FLOAT,
    nMemAvgP95Within    FLOAT,
    nMemMaxP95Within    FLOAT,
    nConnAvgWithin      FLOAT,
    nConnMaxWithin      FLOAT,
    -- Low-CPU projections
    nCpuAvgLowCpu       FLOAT,
    nCpuMaxLowCpu       FLOAT,
    nCpuAvgP95LowCpu    FLOAT,
    nCpuMaxP95LowCpu    FLOAT,
    nMemAvgLowCpu       FLOAT,
    nMemMaxLowCpu       FLOAT,
    nMemAvgP95LowCpu    FLOAT,
    nMemMaxP95LowCpu    FLOAT,
    nConnAvgLowCpu      FLOAT,
    nConnMaxLowCpu      FLOAT
)
GO