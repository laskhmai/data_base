📋 User Story
Title: Create GCP Aggregated Daily Resource Cost Table and Stored Procedure

As a Data Engineer,
I want to create a staging table, final table, and stored procedure for GCP daily resource cost aggregation in the Silver layer,
So that GCP cloud spend data is available in a consistent, aggregated format — matching the pattern already established for Azure.

Acceptance Criteria

 Investigate GCP source data in [Cloudability].[Daily_Spend] where vendor = 'GCP' to identify correct column mappings (resource name, project/resource group, account identifiers, tags)
 Create staging table [Silver].[Cloudability_Daily_Resource_Cost_Staging] (or confirm it is shared with Azure)
 Create or update final table [Silver].[Cloudability_Daily_Resource_Cost] with any GCP-specific columns (e.g. gcp_project, gcp_resource_name)
 Create stored procedure [Silver].[usp_CloudabilityAggregate_DailySpend_GCP] that aggregates GCP daily spend with operation cost, usage family cost, reservation identifier cost, Humana application cost, and Humana resource cost JSON columns
 Validate row counts and spend totals against source [Cloudability].[Daily_Spend] for GCP
 SP filters on vendor = 'GCP' and runs for DATEADD(DAY, -3, GETDATE())
 Unit tested with at least one GCP billing date