I am working on the Cloud 3.0 data migration project.

I have attached the Cloud 3.0 Data Design document. Please study the entire document first before doing anything.

You have access to our current SSMS/SQL Server environment and repository/codebase.

IMPORTANT:
This is DISCOVERY ONLY.
Do NOT modify, insert, update, delete, truncate, create, alter, drop, deploy, execute write-producing stored procedures, or change anything in any database/environment.
Do NOT make production changes.
Only perform safe READ-ONLY investigation using metadata queries and SELECT statements.

My goal is to understand the CURRENT system completely and compare it with the TARGET architecture described in the attached Cloud 3.0 Data Design document.

The target architecture appears to involve:

Azure/source inventory
        ↓
Databricks
        ↓
Bronze
        ↓
Silver
        ↓
CDF / incremental change processing
        ↓
Gold current state + audit/history
        ↓
PostgreSQL serving layer

with Prefect used for orchestration.

Do not assume this interpretation is correct.
Verify it against the attached design document and clearly point out anything that differs.


PHASE 1 — CURRENT DATABASE INVENTORY

Connect to the authorized SSMS/SQL Server environment and identify:

1. Server/database names relevant to this project
2. Schemas
3. Tables
4. Views
5. Stored procedures
6. Functions
7. Triggers
8. Jobs/processes if visible
9. Primary keys
10. Foreign keys
11. Unique constraints
12. Indexes
13. Computed columns
14. Important JSON columns
15. Row counts for relevant tables
16. Created/modified date metadata where available

Do not dump unrelated databases.
First identify which databases/schemas appear relevant to Azure inventory / Cloud 3.0.


PHASE 2 — FIND THE CURRENT INVENTORY DATA MODEL

Identify the current tables/objects responsible for:

- subscriptions
- resource groups
- resources
- resource types
- Azure inventory
- tags
- properties
- deleted resources / soft deletes
- historical/audit information
- recommendation data if it participates in this inventory flow

For each relevant table provide:

Database.Schema.Table
Purpose
Primary key
Natural/business key
Foreign keys
Important columns
Data types
Approximate row count
Upstream source
Downstream consumer

Do not guess purpose. If purpose is inferred from code, explicitly say that it is inferred and show the evidence.


PHASE 3 — TRACE CURRENT DATA FLOW

Trace the current system end-to-end.

I want to know:

Where does Azure/source data first enter our system?

Then:

Source
→ ingestion
→ raw/base tables
→ transformations
→ stored procedures/scripts
→ intermediate/silver-like tables
→ gold/final tables
→ PostgreSQL/API/application/recommendation consumers

For every step identify:

- object/job/script name
- source table(s)
- target table(s)
- transformation performed
- execution order
- dependency
- schedule/trigger if available
- relevant repository/file path
- relevant stored procedure/function
- evidence supporting the relationship

Build an ASCII flowchart of the CURRENT architecture.


PHASE 4 — CODE/REPOSITORY DISCOVERY

Search the authorized codebase for references to the relevant database objects.

Find code responsible for:

- Azure inventory collection
- SQL Server writes
- Databricks
- Bronze
- Silver
- Gold
- Delta tables
- Change Data Feed / CDF
- PostgreSQL
- Prefect
- resource deletion handling
- audit/history
- incremental processing
- MERGE/upsert logic

For each important component provide:

File path
Function/class/job name
What it does
Input
Output
Database/table touched

Do NOT expose passwords, tokens, connection strings, secrets, private keys, or credentials.
Redact sensitive values.


PHASE 5 — COMPARE CURRENT SYSTEM TO CLOUD 3.0 DESIGN

Using the attached design document, create a mapping like:

CURRENT COMPONENT
→ CURRENT PURPOSE
→ CLOUD 3.0 TARGET COMPONENT
→ MIGRATION REQUIRED
→ STATUS / GAP / QUESTION

For example, only if supported by evidence:

Current SQL table
→ current inventory storage
→ Databricks Silver/Gold table
→ migration required
→ transformation differences

Do not force a one-to-one mapping if the new architecture redesigns the data.


PHASE 6 — VERIFY THE TARGET TABLES FROM THE DOCUMENT

From the attached design document, extract all proposed:

Bronze tables
Silver tables
Gold tables
Audit tables
PostgreSQL serving tables

For each target table give:

Table name
Purpose
Primary key
Natural key
Foreign keys
Important columns
Generated columns
CDF enabled or not
Audit/history behavior
Soft-delete behavior
Relationship to other tables

Then compare it with the closest CURRENT database object.


PHASE 7 — IDENTIFY MIGRATION RISKS

Identify, with evidence:

- schema differences
- datatype differences
- missing columns
- renamed columns
- key changes
- relationship changes
- JSON handling differences
- duplicate risks
- NULL handling differences
- soft-delete differences
- historical/audit differences
- timezone/timestamp differences
- incremental/CDF challenges
- PostgreSQL compatibility concerns
- stored procedure logic that must be recreated
- dependencies that could break during migration

Do NOT propose production changes yet.


PHASE 8 — GIVE ME A MIGRATION DISCOVERY REPORT

Return the result in this exact structure:

1. Executive Summary
2. Current Architecture
3. Current Database Inventory
4. Current End-to-End Data Flow
5. Important Tables and Relationships
6. Important Stored Procedures / Jobs / Code
7. Cloud 3.0 Target Architecture from the Document
8. Current → Target Mapping
9. Gaps
10. Risks
11. Unknowns / Questions That Still Need Investigation
12. Recommended Migration Order
13. Evidence Used
14. ASCII Current Architecture Diagram
15. ASCII Target Architecture Diagram
16. ASCII Current → Target Migration Diagram

IMPORTANT:
Separate everything into:

CONFIRMED — directly proven by database/code/document
INFERRED — likely based on evidence but not fully proven
UNKNOWN — insufficient evidence

Do not make assumptions just to complete the report.

Do not change anything in the environment.

If the investigation is too large, do not skip sections.
Perform it in phases and save the findings in a Markdown file named:

cloud3_migration_discovery.md

Update that file as you discover more information so we maintain project context for future sessions.