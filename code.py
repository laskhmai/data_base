-- Remove process-level columns — no longer meaningful at cluster level
ALTER TABLE [Metrics].[MongoDBRightsizingAggregated5Min]
DROP COLUMN ProcessId;

ALTER TABLE [Metrics].[MongoDBRightsizingAggregated5Min]
DROP COLUMN ProcessType;

ALTER TABLE [Metrics].[MongoDBRightsizingAggregated5Min]
DROP COLUMN ReplicaSetName;
GO

-- Same for Hourly table
ALTER TABLE [Metrics].[MongoDBRightsizingAggregatedHourly]
DROP COLUMN ProcessId;

ALTER TABLE [Metrics].[MongoDBRightsizingAggregatedHourly]
DROP COLUMN ProcessType;

ALTER TABLE [Metrics].[MongoDBRightsizingAggregatedHourly]
DROP COLUMN ReplicaSetName;
GO