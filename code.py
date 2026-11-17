<<<<<<< HEAD
<<<<<<< HEAD
SELECT *
FROM corporate_information_technology_raw_dev_000.cldcostop.test_sample_table;
=======
Hi Ramkumar, sure, I can help. Add the provided code in a new file named `servicebus-secrets.tf` under your repository’s `infrastructure` folder, alongside `servicebus.tf`. Keep it outside the existing module block and replace `<your KV Name>` with your actual Key Vault name.

Please share the changes for review before running the pipeline.
>>>>>>> 2b09326 (test)
=======
I want to give you the complete context of the Cloud 3.0 work I am doing before
I ask you to change any code.

Please READ this context carefully and do not start implementation yet.

--------------------------------------------------
PROJECT BACKGROUND
--------------------------------------------------

We are working on a Cloud 3.0 migration.

The current environment has older SQL Server / Azure Synapse based data
collection and processing.

The new direction is to move data engineering workloads toward Databricks
using a Medallion Architecture:

SOURCE / API
     ↓
BRONZE
Raw/landing data
     ↓
SILVER
Cleaned / normalized data
     ↓
GOLD
Business-ready / final data

There may also be PostgreSQL serving layers and Prefect orchestration for some
workloads, but those are not the immediate focus of this Metrics task.

The Databricks DEV environment is already available.

We currently have access to catalogs that approximately correspond to:

RAW DEV     → Bronze
STAGING DEV → Silver
MAIN DEV    → Gold

The schema we have seen is:

cldcostop

However, do NOT create or write to any Databricks tables yet.

--------------------------------------------------
MY CURRENT RESPONSIBILITY
--------------------------------------------------

My work is mainly around:

1. Metrics collection
2. Later, Recommendation migration / optimization

Right now, we are concentrating on Metrics first.

The team has told me that Metrics collection logic needs to be migrated toward
the new Databricks Medallion architecture.

However, the final Metrics data design is NOT confirmed to me yet.

Some other team members / architects are working on:

- Metrics data design
- Bronze/Silver/Gold table design
- deciding what exact tables/columns should exist
- potentially creating or provisioning those tables
- defining the approved deployment/process details

Therefore:

I should NOT invent the production Metrics Bronze/Silver/Gold schema.

I should NOT create Databricks tables based on assumptions.

I should NOT start inserting data into Databricks yet.

--------------------------------------------------
WHAT WE WANT TO DO FIRST
--------------------------------------------------

Before the final Metrics design is available, I want to understand and test
the existing Metrics collection logic.

We have an existing Python file:

metrics_sql.py

This is the current/legacy Azure SQL Database Metrics collector.

The current flow appears to be approximately:

Metrics.Settings
       +
AZURE.vw_DBInventory
       ↓
metrics_sql.py
       ↓
Azure authentication
       ↓
Azure Monitor Metrics API
       ↓
Metrics response
       ↓
Pandas/DataFrame
       ↓
CSV
       ↓
Azure Storage / Staging

We want to understand this old flow first.

Then we want to create a NEW fetch-only version of the collector.

The immediate goal is only:

Azure API
     ↓
Fetch metric data
     ↓
Parse response
     ↓
Create DataFrame
     ↓
Print/display DataFrame
     ↓
STOP

No inserts.

No database writes.

No Databricks writes.

No Azure Storage uploads.

--------------------------------------------------
WHAT SHOULD REMAIN FROM THE OLD COLLECTOR
--------------------------------------------------

The existing code contains useful logic around:

- Azure authentication
- access-token generation
- token refresh
- Azure Monitor Metrics API calls
- time-range calculation
- retries
- resource iteration
- concurrency/threading
- metric configuration
- metric-response parsing

We want to understand which of these parts can be reused safely.

--------------------------------------------------
WHAT SHOULD NOT BE CARRIED FORWARD AS-IS
--------------------------------------------------

We should not carry forward:

- hardcoded secrets
- hardcoded passwords
- printed access tokens
- insecure credential handling
- unnecessary SQL Server-specific output logic
- CSV/Storage upload logic if the new architecture no longer requires it

Secrets must never be embedded directly in code.

Use environment variables/runtime input or the company's approved secret
mechanism.

--------------------------------------------------
IMPORTANT DEVELOPMENT APPROACH
--------------------------------------------------

We are doing this in small steps.

STEP 1:
Understand current metrics_sql.py

STEP 2:
Create a sanitized FETCH-ONLY Python version

STEP 3:
Test Azure authentication

STEP 4:
Test fetching a very small number of Azure SQL Database metrics

STEP 5:
Convert the response to a DataFrame and print it

STEP 6:
Compare the fetched data with the Metrics Bronze data design once that design
is provided by the architecture/team

STEP 7:
Only after design confirmation, implement Bronze write logic

STEP 8:
Later implement Silver and Gold transformations

STEP 9:
Later integrate deployment/orchestration through the approved GitHub /
GitHub Actions / Databricks process

--------------------------------------------------
IMPORTANT LIMITATIONS
--------------------------------------------------

For the current phase:

- do not write to SQL Server
- do not write to Azure Synapse
- do not write to Databricks
- do not write Delta tables
- do not create tables
- do not alter schemas
- do not upload CSVs
- do not upload to ADLS
- do not deploy anything
- do not modify production systems

This is an investigation + API-fetch proof-of-concept only.

--------------------------------------------------
YOUR ROLE
--------------------------------------------------

I will use you to help me:

1. Understand the current Metrics code
2. Identify reusable logic
3. Create a clean fetch-only version
4. Test the Azure Metrics API safely
5. Understand the returned data
6. Later compare the response with the official Metrics data design
7. Later help migrate the flow into Databricks Medallion architecture

If you understand the above context, summarize back to me:

- What the current system is
- What Cloud 3.0 is trying to change
- What my immediate task is
- What the other team still needs to provide
- What we should NOT do yet

Do NOT write or modify code yet.
>>>>>>> 8ab5e5b (test)
