Current Cloudability Flow
Working notes based on the Cloudability code and SQL objects reviewed so far.
                         CLOUDABILITY
                              |
                              | API
                              v
          +-------------------+-------------------+
          |                   |                   |
          v                   v                   v
        SPEND                TAGS             PROPERTIES
          |                   |                   |
          v                   v                   v
 CloudabilitySpend.py  Cloudability_tags.py   properties.py
          |                   |                   |
          v                   v                   v
      Spend CSV            Tags CSV         Properties CSV
          |                   |                   |
          +-------------------+-------------------+
                              |
                              v
                    ADLS / Azure Storage
                              |
                 +------------+------------+
                 |                         |
                 v                         v
          External Tables             Staging Tables
                 |                         |
                 +------------+------------+
                              |
                              v
                  Collector / Upsert SPs
                              |
          +-------------------+-------------------+
          |                   |                   |
          v                   v                   v
 Cloudability.Spend   Cloudability.Tags   Cloudability.Properties
                              |
                     + Cloudability.Categories
                              |
                              v
                 Daily_Spend_Aggregate
                              |
                              v
                 Cloudability.Daily_Spend
Questions to confirm
1. What process populates Staging.Spend, Staging.Properties, Staging.Categories, and Staging.Tags? Trace each table backward to its actual source. Determine whether the source is Cloudability API directly, ADLS CSV/files, Synapse/ADF Copy Activity, Python/runbook, notebook, or another process. Also identify the pipeline/job name, source path/API, trigger/schedule, and load method. Do not assume - show evidence.
2. Which Synapse/ADF pipeline or job calls the Cloudability collector/upsert stored procedures and Daily_Spend_Aggregate? Show the activity order, trigger/schedule, and which procedure is called at each step.
Main tables and procedures
Type	Objects
External tables	ExtTbl_Cloudability_Spend, ExtTbl_Cloudability_Properties, ExtTbl_Cloudability_Categories, ExtTbl_Cloudability_Tags
Staging tables	Staging.Spend, Staging.Properties, Staging.Categories, Staging.Tags
Cloudability tables	Cloudability.Spend, Cloudability.Properties, Cloudability.Categories, Cloudability.Tags
Stored procedures	usp_Spend_Collector_Insert, usp_Spend_Staging_to_SQL, usp_Properties_Collector_Upsert, usp_Categories_Collector_Upsert, usp_Tags_Collector_Upsert
Final	Cloudability.Daily_Spend_Aggregate -> Cloudability.Daily_Spend


FOCUS POC
What is FOCUS?
FOCUS (FinOps Open Cost and Usage Specification) is a common standard for cloud cost and usage data. It gives a consistent structure for cost and usage information instead of depending only on provider-specific billing formats.
What are we trying to check?
We want to compare FOCUS cost and usage data with our current Cloudability.Daily_Spend output. We need to understand what maps directly, what needs transformation, and what company-specific information still needs separate enrichment.
                 Azure Cost Management
                          |
                          v
                FOCUS format export
                          |
                          v
                   ADLS / Storage
                          |
                          v
              SQL / Synapse POC load
                          |
                          v
                    FOCUS dataset
                          |
                          v
             Compare / reconcile with
                          |
                          v
              Cloudability.Daily_Spend
                          |
                          v
       Identify mappings, gaps and enrichment
Question to confirm for FOCUS
For our FOCUS POC, where are we getting the FOCUS dataset from? Are we going to configure an Azure Cost Management FOCUS export to ADLS, or is there already a FOCUS dataset/export available internally? If it already exists, please show the source, storage location/table and how it is loaded.
Note: The FOCUS flow above is the working POC direction. We should update it after the actual FOCUS source and loading process are confirmed.