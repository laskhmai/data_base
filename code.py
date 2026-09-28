Yes, continue — but before doing deeper analysis, I want to correct and expand
the discovery scope based on what I manually verified in SSMS.

The previous discovery focused heavily on the AZURE schema. I have now confirmed
that hybridasa_dedicatedpool contains many other potentially relevant schemas
and tables.

I can see areas including:

- Analytics
- ASTRA
- Automation
- AZURE
- Cloudability
- CostAtlas
- dbo
- Silver
- SNOW
- Staging
- Metrics-related tables
- Turbonomic/Turbonucs-related tables
- and potentially other schemas not yet reviewed

So DO NOT assume AZURE is the complete source for the Cloud 3.0 migration.

Our goal is still the same:

Understand the CURRENT system accurately and compare it against the TARGET
architecture defined in Inventory_dataDesign_1.md.

TARGET DESIGN:

Azure Resource Graph / ARM APIs
        ↓
Bronze - Databricks Delta raw ingestion
        ↓
Silver - Databricks Delta normalized data
        ↓
CDF - detect changed records
        ↓
Gold - current trusted state + bounded audit/history
        ↓
PostgreSQL - application/API serving layer

Prefect will orchestrate the overall process.

--------------------------------------------------
PHASE 1 — COMPLETE CURRENT-STATE INVENTORY
--------------------------------------------------

First perform a READ-ONLY inventory of hybridasa_dedicatedpool.

Get the complete list of schemas and tables.

For each table collect metadata where safely available:

- schema
- table name
- columns
- data types
- approximate/current row count
- obvious primary/unique/business keys
- obvious timestamp/load/audit columns
- whether it appears active or historical/archive/staging

Do NOT dump all table data.

Use INFORMATION_SCHEMA, sys catalog views, metadata and safe SELECT queries.

--------------------------------------------------
PHASE 2 — FIND CLOUD 3.0 RELEVANT OBJECTS
--------------------------------------------------

From the complete inventory, identify objects related to:

- Azure resource inventory
- Resources
- Subscriptions
- Resource Groups
- VMs
- Disks
- App Services
- Metrics
- Staging
- Silver/normalized data
- Gold/current-state data
- Turbonomic
- Cloudability
- Recommendations
- resource properties/tags
- inventory history/archive

Do not classify a table as relevant only because its name contains a keyword.

Use actual schema/column/dependency evidence.

--------------------------------------------------
PHASE 3 — TRACE CURRENT DATA FLOW
--------------------------------------------------

For relevant objects, determine as much as possible:

SOURCE
   ↓
COLLECTOR / LOADER / PROCEDURE / JOB
   ↓
STAGING
   ↓
TRANSFORMATION
   ↓
CURRENT/FINAL TABLE
   ↓
DOWNSTREAM CONSUMER

Inspect metadata, views, stored procedures, functions and dependency information
where available.

I specifically want to understand:

WHO WRITES → TABLE → WHO READS

Do NOT guess lineage based only on table names.

If evidence proves something, mark:
CONFIRMED

If evidence strongly suggests something but does not prove it, mark:
INFERRED

If we cannot determine it, mark:
UNKNOWN

--------------------------------------------------
PHASE 4 — COMPARE CURRENT SYSTEM TO TARGET DESIGN
--------------------------------------------------

Compare the discovered CURRENT architecture with Inventory_dataDesign_1.md.

Map, where evidence supports it:

CURRENT                         TARGET
------------------------------------------------
Current raw ingestion      →    Bronze
Current normalization      →    Silver
Current change tracking    →    CDF
Current final/current data →    Gold
Current serving layer      →    PostgreSQL
Current scheduling/jobs    →    Prefect

Do NOT force a mapping.

If the current system has no equivalent, say NONE FOUND.
If unclear, say UNKNOWN.

Also identify:

1. What current functionality must be preserved.
2. What functionality is intentionally changing.
3. What old tables/processes may no longer be needed.
4. What dependencies could break during migration.
5. What questions still need confirmation from the team.

--------------------------------------------------
IMPORTANT EXECUTION RULE
--------------------------------------------------

Do this incrementally.

Do NOT immediately deeply scan thousands of tables.

First:
1. Inventory schemas/tables.
2. Narrow to relevant candidates.
3. Show the candidate list and reasoning.
4. Then continue deeper dependency/lineage analysis.

You may continue automatically with safe read-only metadata discovery.
For any expensive/full-table scan, destructive action, or uncertain operation,
STOP and ask me first.

--------------------------------------------------
FILE MANAGEMENT RULE — IMPORTANT
--------------------------------------------------

Do NOT create a new .py file for every discovery step.

Use ONE Python discovery script only:

cloud3_migration_discovery.py

Keep extending/refactoring that same script.

Use ONE documentation file:

cloud3_migration_discovery.md

Use at most ONE reusable raw/log output file if necessary:

cloud3_discovery_output.txt

Do not create phase1.py, phase2.py, targeted.py, check.py, etc.

If temporary files from previous discovery already exist, do not delete them
without asking me first.

--------------------------------------------------
SAFETY
--------------------------------------------------

DATABASE ACCESS IS STRICTLY READ-ONLY.

Allowed:
SELECT
INFORMATION_SCHEMA
sys catalog views
metadata/dependency inspection

Not allowed:
INSERT
UPDATE
DELETE
MERGE
DROP
ALTER
TRUNCATE
CREATE database objects
EXECUTE business procedures
or anything that modifies production data/schema.

Do not modify the database.

Start now with the complete schema/table inventory and candidate identification.
Then show me:

A. What you discovered
B. Which schemas/tables look relevant to Cloud 3.0
C. Why each is relevant
D. What is still UNKNOWN
E. What you recommend inspecting next

Do not start migration implementation yet.
We are still in DISCOVERY / CURRENT-STATE UNDERSTANDING.