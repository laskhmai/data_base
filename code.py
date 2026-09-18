Please trace the complete upstream lineage of the StorageMax column in [Metrics].[SqlDataBasesAggregatedHourly]. Use read-only investigation only.

Search all available notebooks, Python files, SQL scripts, stored procedures, views, and functions for StorageMax and SqlDataBasesAggregatedHourly.
Identify exactly where StorageMax is inserted or updated.
Check whether it comes from an API response, another source table, or a calculated field.
If it comes from an API, identify the API field name and the code that maps it to StorageMax.
Compare the upstream process before and after July 13, 2026, when the value became completely NULL.
Check pipeline/job execution history around July 12–13 for failures or deployment changes.
Provide the complete lineage: source/API → ingestion job → transformation → [Metrics].[SqlDataBasesAggregatedHourly].[StorageMax].

Do not modify or rerun any production process. Clearly separate confirmed evidence from assumptions.