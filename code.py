Good. Your understanding of Inventory_dataDesign_1.md is now the TARGET design context.

Before continuing, I want to correct the discovery approach.

Do NOT assume that every object containing words such as Azure, resource, tag, cloud, cost, billing, etc. belongs to this migration.

Our immediate goal is NOT to inventory the entire SQL Server.

Our goal is to identify the CURRENT implementation that corresponds specifically to the Azure inventory domain described in Inventory_dataDesign_1.md.

Please proceed READ-ONLY only. No writes or database changes.

STEP 1:
Run your targeted AZURE-schema discovery.

Start specifically with the objects that appear to correspond to:

- Resources
- Subscriptions
- Resource Groups
- Resource Type

Also identify closely related objects only when there is actual dependency evidence.

STEP 2:
For those objects, collect:

- exact database.schema.object name
- object type (table/view/etc.)
- columns and data types
- primary key
- foreign keys
- indexes
- computed columns
- approximate/exact row count where safe
- sample 5 rows, but redact any sensitive information if present

STEP 3:
Trace how data gets INTO these current objects.

Search stored procedures, views, functions, SQL Agent jobs, Python/code repository references, pipelines, or other code that INSERTs, UPDATEs, MERGEs, or otherwise populates them.

I specifically need to know:

Azure/source
   ↓
collector/process
   ↓
which current SQL object
   ↓
which transformation/process
   ↓
next object
   ↓
final consumer

Do not infer this only from table names.
Show evidence from actual dependencies/code.

STEP 4:
Trace how data gets OUT of these objects.

Identify downstream:
- stored procedures
- views
- applications/APIs
- recommendation processes
- other databases
- jobs

Again, only include relationships supported by evidence.

STEP 5:
Now compare ONLY these confirmed current Azure inventory objects with the target design in Inventory_dataDesign_1.md.

Create a table:

Current Object
Current Purpose
Current Key
Current Upstream
Current Downstream
Target Bronze Object
Target Silver Object
Target Gold Object
Target Postgres Object
Confirmed / Inferred / Unknown
Migration Concern

IMPORTANT:

Do not start migration.
Do not generate implementation code.
Do not modify the database.
Do not design missing architecture yourself.

At this stage we are building an evidence-based CURRENT → TARGET map.

Also, do not assume Prefect is currently implemented merely because it is part of our broader Cloud 3.0 discussion. Verify Prefect from repository/configuration evidence separately.

Save all confirmed findings into cloud3_migration_discovery.md.

At the end, give me:

1. What you CONFIRMED
2. What you INFERRED
3. What is still UNKNOWN
4. Current architecture ASCII diagram
5. Target architecture ASCII diagram
6. Current → Target mapping
7. The next 5 questions we need to answer before migration can begin

If a query is slow or times out, stop that query and use metadata/estimated counts instead of stressing the database.



IMPORTANT FILE MANAGEMENT RULE:

Do NOT create a new .py file for every investigation step.

I noticed you are currently creating multiple temporary Python files for individual steps/checks. I do not want this because it is cluttering my local workspace.

From now on:

1. Maintain ONE Python discovery script only:

   cloud3_migration_discovery.py

2. Put all database discovery/investigation logic into this single file.

3. Organize the file using clearly named functions, for example:

   check_connection()
   discover_azure_schema()
   discover_tables()
   discover_columns()
   discover_primary_keys()
   discover_foreign_keys()
   discover_indexes()
   discover_row_counts()
   discover_dependencies()
   discover_stored_procedures()
   trace_upstream()
   trace_downstream()

4. When we need another discovery step, UPDATE the existing
   cloud3_migration_discovery.py file instead of creating another .py file.

5. If some previous temporary discovery scripts were already created,
   first list them for me.

   DO NOT delete them automatically.

   Tell me which files appear to be temporary/redundant and wait for my
   approval before deleting anything.

6. Keep the documentation/report separately in:

   cloud3_migration_discovery.md

So ideally this investigation should maintain only:

   cloud3_migration_discovery.py   <-- all read-only discovery code
   cloud3_migration_discovery.md   <-- findings/documentation

7. Do NOT create additional .py, .sql, .txt, .json, or temporary files
   unless there is a genuine technical requirement.

8. If you believe another file is necessary, explain WHY and ask me
   before creating it.

9. Continue to enforce READ-ONLY database access in the Python script.
   No INSERT, UPDATE, DELETE, MERGE, CREATE, ALTER, DROP, TRUNCATE,
   or other database-changing operations.

10. Before executing each new discovery phase, modify/reuse the existing
    cloud3_migration_discovery.py rather than generating a new script.

Please confirm this file-management rule and then continue with the
targeted Azure inventory discovery.