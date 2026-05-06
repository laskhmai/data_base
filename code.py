MongoDB Rightsizing — Aggregation Metrics Stored Proc & Hourly Table

As part of MongoDB right-sizing initiative, implement hourly aggregation 
of process-level metrics into a reporting table following the same pattern 
as the PostgreSQL rightsizing stored proc.

WHAT WAS BUILT:
1. Created [Metrics].[MongoDBRightsizingAggregatedHourly] table
2. Created [Metrics].[usp_MongoDBRightsizingAggregatedMetrics] stored proc

TABLE: 32 columns — Identity, SKU, Time, CPU, Memory, Network, 
Connections, Opcounters. One row per process per hour.

STORED PROC: Reads from 12 MongoDB metric tables (15M interval), 
aggregates to hourly buckets in EST, joins to [MongoDB].[Process] 
and [MongoDB].[Clusters] for inventory context, upserts into target table.

METRICS COVERED:
- CPU Avg + Max (with Gt50, Gt25, Gt10 threshold counts)
- Memory Resident Max + Avg (from _5M — 15M table is disabled)
- Memory Available Min
- Network In/Out Avg + Max
- Network Num Requests Max
- Connections Max + Avg
- Opcounter Query + Insert Max

KEY DESIGN DECISIONS:
- Process level (not cluster level) — keeps PRIMARY vs SECONDARY split
- InstanceSize from effectiveElectableSpecs JSON path in ReplicationSpecs
- CPU thresholds 50/25/10 (not 88) — normalized CPU specific to MongoDB
- Memory Resident uses 5M table (15M is disabled in MongoDbSettings)

✅ CREATE TABLE runs without errors
✅ Stored proc executes successfully
✅ Data validated — aggregation table matches raw source calculations
✅ All 12 metric CTEs populating correctly
✅ Network columns (NetInAvg, NetInMax, NetOutAvg, NetOutMax) populated
✅ InstanceSize correctly parsed from ReplicationSpecs JSON
✅ UTC → EST hour bucket conversion verified