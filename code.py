I want to trace the Cloudability data flow using ONLY the access you currently have.

IMPORTANT:
- Do NOT assume you have Azure Portal, Synapse workspace, Azure Storage, or pipeline access.
- Do NOT ask for or attempt to obtain additional credentials.
- Use only currently authorized resources such as SSMS/database access and accessible repositories/files.
- READ ONLY.
- Do NOT INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, execute production procedures, or trigger jobs/pipelines.
- Do not modify any code.

BACKGROUND:

We know Cloudability data is extracted into separate feeds:
- Spend
- Tags
- Properties
- Categories

From local Python code we already know Spend, Tags and Properties are fetched from Cloudability APIs and CSV files are uploaded to Azure storage/staging locations.

Our final table of interest is:

Cloudability.Daily_Spend

For now, I want to work BACKWARDS from Daily_Spend using SSMS.

TASK 1 — Find Daily_Spend

Identify:
- Database
- Schema
- Exact object name
- Object type (table/view/etc.)
- Columns
- Primary keys/indexes if available

TASK 2 — Find everything that references Daily_Spend

Search SQL Server metadata for:
- Stored procedures
- Views
- Functions
- Triggers
- SQL objects

that reference:
Cloudability.Daily_Spend
or Daily_Spend.

Show:
Object name | Object type | Schema | Relevant SQL definition

TASK 3 — Determine what populates Daily_Spend

Find whether Daily_Spend is populated by:
- INSERT
- INSERT SELECT
- MERGE
- stored procedure
- another table
- view
- dynamic SQL

If you find the object that populates it, inspect that object's definition.

DO NOT EXECUTE the procedure.

TASK 4 — Trace backwards recursively

For every upstream table/view referenced by the process that populates Daily_Spend:

1. Identify that object.
2. Find what populates that object.
3. Continue backwards.

Pay particular attention to names containing:

Cloudability
Spend
Daily_Spend
Daily_Spend_Aggregate
Tags
Properties
Categories
Staging
Stage
Raw

TASK 5 — Find Cloudability-related database objects

Search SSMS metadata and list ALL accessible database objects whose names or definitions contain:

Cloudability
Spend
Tags
Properties
Categories

Group them by:
Tables
Views
Stored Procedures
Functions
Other objects

TASK 6 — Investigate joins

When you find SQL combining Cloudability data, report:

- Left table
- Right table
- Join type
- Join keys
- Filters
- Deduplication/grouping
- Result/destination

Specifically look for keys such as:

resource_identifier
vendor_account_name
date
invoice_date

TASK 7 — Column lineage

For Cloudability.Daily_Spend, trace as many columns as possible back to their immediate source.

Output:

Daily_Spend column
→ Source table
→ Source column
→ Transformation if any

Do NOT guess if the source cannot be proven.

TASK 8 — Identify the boundary of our current access

Eventually we may reach a staging/raw/external table whose population happens outside SQL Server.

When that happens, STOP the backward trace there and tell me:

"This is the earliest point that can currently be proven from SSMS."

Do not invent the Azure-side process.

FINAL OUTPUT:

Give me:

1. Confirmed backward lineage diagram:

Cloudability.Daily_Spend
        ↑
[procedure/transformation]
        ↑
[upstream table]
        ↑
[procedure/transformation]
        ↑
[earliest object visible from SSMS]

2. Evidence table:

Step | Object | Object Type | Reads From | Writes To | Evidence

3. List of joins discovered.

4. Column lineage discovered.

5. Earliest point visible from SSMS.

6. Missing pieces that require Azure/Synapse/Storage access.

7. Exact SQL object names and relevant SQL snippets used as evidence.

Do not make any changes. This is investigation only.