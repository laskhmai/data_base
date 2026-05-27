-- CPU P95
ALTER TABLE [Metrics].[MongoDBRightsizingAggregatedHourly]
ADD CpuMaxP95           FLOAT   NULL;

ALTER TABLE [Metrics].[MongoDBRightsizingAggregatedHourly]
ADD CpuAvgP95           FLOAT   NULL;

-- Memory %
ALTER TABLE [Metrics].[MongoDBRightsizingAggregatedHourly]
ADD MemResidentMaxPct   FLOAT   NULL;

ALTER TABLE [Metrics].[MongoDBRightsizingAggregatedHourly]
ADD MemResidentAvgPct   FLOAT   NULL;

ALTER TABLE [Metrics].[MongoDBRightsizingAggregatedHourly]
ADD MemResidentP95Pct   FLOAT   NULL;

-- Connection %
ALTER TABLE [Metrics].[MongoDBRightsizingAggregatedHourly]
ADD ConnUtilizationPct  FLOAT   NULL;
GO

-- Verify
SELECT
    ORDINAL_POSITION AS [#],
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'MongoDBRightsizingAggregatedHourly'
ORDER BY ORDINAL_POSITION
GO