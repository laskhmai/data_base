I am working on a POC to understand our existing Cloudability cost-data pipeline.

IMPORTANT:
- This is READ-ONLY analysis.
- Do NOT modify any database objects, tables, stored procedures, jobs, code, configuration, or data.
- Do NOT run INSERT, UPDATE, DELETE, ALTER, DROP, CREATE, TRUNCATE, MERGE, or any deployment.
- Only inspect and report findings.

Please connect to the authorized SQL Server/database environment and identify the table that stores our Cloudability daily spend/cost data.

I believe there is an existing table related to "Cloudability" and "Daily Spend", but do not assume the exact table name.

STEP 1 — FIND THE TABLE
Search database metadata for tables/views containing names related to:
- Cloudability
- Daily Spend
- DailySpend
- Spend
- Cost

Return:
1. Database name
2. Schema name
3. Exact table/view name
4. Why you believe this is the Cloudability Daily Spend table

STEP 2 — SHOW THE SCHEMA
For the most likely table, return ALL columns with:
- Column name
- Data type
- Max length/precision/scale where applicable
- Nullable / Not Nullable
- Primary key information if available

STEP 3 — SHOW SAMPLE DATA
Return only 5 recent rows from the table.
Mask/redact any secrets, credentials, tokens, personal information, or other sensitive values.

STEP 4 — UNDERSTAND DATE AND COST FIELDS
Identify which columns appear to represent:
- Spend/usage date
- Cost/spend amount
- Resource ID
- Resource name
- Subscription/account
- Service
- Resource type
- Region/location
- Tags
- Currency
- Quantity/usage, if available

Do not guess. If a field's meaning is uncertain, mark it as "Needs confirmation."

STEP 5 — CHECK DATA AVAILABILITY
Using the identified date column, report:
- MIN(date)
- MAX(date)
- number of distinct dates
- row count for the latest available date

STEP 6 — FIND HOW THIS TABLE IS POPULATED
Search code/database dependencies for anything that writes to or loads this table.

Look for:
- Stored procedures
- SQL jobs
- Python code
- ETL pipelines
- loaders
- APIs
- Cloudability references

Return the names/locations only with relevant evidence.
Do not change or execute the ingestion process.

FINAL OUTPUT

Give me a structured report:

A. Exact Daily Spend table
B. Full column schema
C. 5 recent sample rows
D. Important cost/resource/tag/date columns
E. Data date range
F. What appears to populate this table
G. Any uncertainties requiring further investigation

Again: READ-ONLY investigation only.