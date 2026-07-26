GCP Cloudability Aggregated Cost
Full Project Summary — Pin to Pin
Date: July 24, 2026  |  Developer: Jayanth  |  Manager: Neeraja Katha
1. Background & Manager Instructions
Manager Neeraja Katha requested creation of GCP Cloudability Aggregated Cost table and stored procedure, following the same pattern already established for Azure.

Neeraja's Chat Instructions (2:55 PM):
"Hi Jayanth, Could you create the same table and SP for GCP resources as well - Aggregated cost. If you are free create a story for that and tag the parent to respective on that you have worked on earlier for Azure. Just make sure mapping is proper. The columns may change example resourcegroups or account. First take a resource see it in the GCP DB and based on that create for aggregated cost. Any questions ping me?"

Key Points from Neeraja:
●Same structure as Azure — just map columns correctly for GCP
●Check GCP columns first — resource group or account may differ
●First look at GCP data in DB, then build the SP
●GCP does NOT have tags like Azure
●Mapping will be difficult — GCP has no resource group, no subscription
●First implement basic table, then validate
●Check what is missing — if not much → leave it, if too much → investigate

2. Understanding the Existing Azure SP
What the Azure SP Does:
Takes raw daily cloud spend rows from [Cloudability].[Daily_Spend] and aggregates them into ONE row per resource per day per vendor — then upserts into the Silver layer table.

Raw vs Silver Example:

date	resource_id	Operation	amortized_spend
2026-07-20	vm-prod-001	ComputeHR	120.00
2026-07-20	vm-prod-001	Storage	38.70
2026-07-20	vm-prod-001	Bandwidth	5.00
2026-07-20	vm-prod-001	Licensing	10.00

👆 4 RAW rows → becomes ONE Silver row with JSON columns:

billing_date	resource_id	overall_spend	operation_cost (JSON)
2026-07-20	vm-prod-001	173.70	{"ComputeHR":120.00,"Storage":38.70,"Bandwidth":5.00}

Azure SP Steps (13 Steps):
●Step 1: Truncate staging table
●Step 2: base CTE — filter vendor=Azure, date=3 days ago, clean resource_id using STUFF+CHARINDEX to remove GUID prefix
●Step 3: parent CTE — ONE row per resource (SUM spend, MAX names)
●Step 4: Operation Cost JSON — SUM spend per operation, STRING_AGG into JSON
●Step 5: Operation Usage JSON — SUM quantity per operation
●Step 6: Usage Family Cost JSON — SUM spend per usage_family
●Step 7: Usage Family Quantity JSON — SUM quantity per usage_family
●Step 8: Usage Types — DISTINCT list, comma separated
●Step 9: Reservation Identifier Cost JSON
●Step 10: Humana Application Cost JSON
●Step 11: Humana Resource Cost JSON
●Step 12: INSERT into staging table
●Step 13: UPDATE existing rows in main table
●Step 14: INSERT new rows only (WHERE NOT EXISTS)

Azure resource_id format:
251460e0-ac68-4e30-9f8d-eb0e2d77b03f :/subscriptions/185d4a23-095e-4401/resourcegroups/adb-udap/providers/microsoft.compute/disks/...
→ Has GUID prefix before /subscriptions → STUFF needed to remove it

3. GCP Data Investigation
GCP Hierarchy vs Azure:

Level	Azure	GCP
Top Level	Subscription	Organization
2nd Level	Resource Group	Folder (optional)
3rd Level	Resource	Project
Bottom	Resource Type	Resource

GCP resource_id format found:
//compute.googleapis.com/projects/609524428043/zones/us-central1/instances/419142804998531516
//bigquery.googleapis.com/projects/book-of-business-report-374620/datasets/book_of_business_report
//alloydb.googleapis.com/projects/320144109268/locations/us-east4/backups/automated-bkp-...
→ NO GUID prefix → NO STUFF needed for GCP!

GCP Column Mapping Confirmed:

Azure Column	GCP Column	GCP Source	Status
azure_resource_name	gcp_resource_name	Azure_Resource_Name column	✅ Has data
azure_resource_group	gcp_project	vendor_account_name	✅ Has data
Azure_Resource_Group(tag11)	N/A	NULL for GCP	❌ Not available
region	region	region column	⚠️ Some (not set)
Humana_Application_ID	same	same column	⚠️ Partial
Humana_Resource_ID(tag23)	same	same column	⚠️ Partial

GCP Services Found (77 Total):

Service	Resources	Total Spend
GCP Vertex AI	112	$136,368
GCP Compute Engine	69,571	$46,896
GCP Cloud Dialogflow API	14	$44,765
GCP Support	3	$18,912
GCP App Engine	17	$17,800
GCP Cloud Logging	893	$14,069
GCP BigQuery	36,443	$1,635
NULL service_name	88,648	$4,378

4. GCP Tables Created
Tables:
●[Silver].[Cloudability_Daily_Resource_Cost_GCP_Staging]
●[Silver].[Cloudability_Daily_Resource_Cost_GCP]

Table Structure:

Column	Data Type	Notes
billing_date	DATE	Partition key
resource_id	NVARCHAR(1000)	GCP resource path
vendor_account_name	NVARCHAR(500)	GCP Project name
vendor	NVARCHAR(50)	GCP
overall_amortized_spend	DECIMAL(38,6)	Changed from FLOAT
overall_usage_quantity	DECIMAL(38,10)	Changed from FLOAT — large values
operation_cost	NVARCHAR(MAX)	JSON
operation_usage	NVARCHAR(MAX)	JSON
gcp_resource_name	NVARCHAR(500)	Replaces azure_resource_name
gcp_project	NVARCHAR(500)	Replaces azure_resource_group
service_name	NVARCHAR(500)	GCP service
usage_family_cost	NVARCHAR(MAX)	JSON
usage_family_quantity	NVARCHAR(MAX)	JSON
usage_types	NVARCHAR(MAX)	Comma separated
vendor_account_identifier	NVARCHAR(500)	Account ID
region	NVARCHAR(200)	GCP region/zone
updated_date	DATE	Source update date
last_modified_date	DATE	SP run date
reservation_identifier_cost	NVARCHAR(MAX)	JSON — mostly (not set) for GCP
humana_applicationid_cost	NVARCHAR(MAX)	JSON
humana_resourceid_cost	NVARCHAR(MAX)	JSON

Synapse Requirements:
WITH ( DISTRIBUTION = ROUND_ROBIN, HEAP )
HEAP required because NVARCHAR(MAX) columns exist. No PRIMARY KEY — Synapse does not enforce it.

5. GCP Stored Procedure
SP Name: [Silver].[usp_CloudabilityAggregate_DailySpend_GCP]

Key Differences from Azure SP:

Item	Azure SP	GCP SP
Vendor filter	vendor = 'Azure'	vendor = 'GCP'
resource_id	STUFF to remove GUID prefix	Direct — no change needed
Resource name col	azure_resource_name	gcp_resource_name
Resource group col	azure_resource_group	gcp_project
Project source	Azure_Resource_Group(tag11)	vendor_account_name
Target tables	_Cost / _Staging	_Cost_GCP / _GCP_Staging
SP Name	usp_...DailySpend	usp_...DailySpend_GCP

Issues Found and Fixed:

Issue	Cause	Fix
Arithmetic overflow error	usage_quantity had 1.26E+20 — too big for DECIMAL(18,6)	Changed to DECIMAL(38,10)
Scientific notation (E) in results	FLOAT columns show 5.6E-005 etc	CONVERT(DECIMAL(38,6), value) in all JSON
Negative amortized_spend	GCP has -1.2E-05 values	DECIMAL handles negatives fine

Fix Applied in base CTE:
amortized_spend = ROUND(ISNULL(CONVERT(DECIMAL(38,6), s.amortized_spend), 0.0), 6)
usage_quantity  = ROUND(ISNULL(CONVERT(DECIMAL(38,10), s.usage_quantity), 0.0), 10)

Fix Applied in all JSON STRING_AGG:
CONVERT(VARCHAR(50), CONVERT(DECIMAL(38,6), op_spend))
CONVERT(VARCHAR(50), CONVERT(DECIMAL(38,10), op_qty))

6. Validation Results — Date: 2026-07-15
Row Count & Spend Validation:

Check	Value	Status
RAW row count	125,478	✅
SILVER row count	97,673	✅ Collapsed!
RAW total spend	110,203.500086	✅
SILVER total spend	110,203.500086	✅ Exact Match!

125,478 raw rows → 97,673 Silver rows — SP successfully collapsed multiple rows per resource into ONE row. Total spend matches exactly — no data lost!

7. Tag Coverage Analysis
Overall Tag Coverage:

Metric	Count	Percentage	Status
Total Resources	97,673	100%	
Missing App ID	20,534	21%	⚠️ Needs attention
Has App ID	77,139	79%	✅ Good
Missing Resource ID	3,399	3%	✅ Acceptable
Has Resource ID	94,274	97%	✅ Good
Missing GCP Resource Name	0	0%	✅ Perfect
Missing Region	58,787	60%	⚠️ Expected for BigQuery/serverless

Missing Tags by Service (Top Services):

Service	Total Resources	Missing App ID	Missing Resource ID
NULL service	55,898	4,230	468
GCP Compute Engine	30,088	11,473	2,213
GCP Networking	3,951	2,245	125
GCP Cloud Storage	2,417	517	189
GCP Cloud SQL	1,384	750	57
GCP Secret Manager	1,092	326	88
GCP Cloud Logging	878	355	70
GCP BigQuery	655	202	38

Key Observations:
●GCP Compute Engine has highest missing App ID (11,473) — GCP resources lack proper tagging
●60% missing region is EXPECTED — BigQuery and serverless services use "(not set)"
●0% missing GCP Resource Name — good data quality here
●97% have Resource ID — very good coverage
●As Neeraja expected — GCP has less tagging than Azure

8. Current Status & Next Steps

Step	Task	Status
1	Understand GCP data structure	✅ Done
2	Investigate GCP columns & hierarchy	✅ Done
3	Create GCP Staging & Main tables	✅ Done
4	Create GCP Stored Procedure	✅ Done
5	Fix arithmetic overflow error	✅ Done
6	Fix scientific notation (E) issue	✅ Done
7	Load one day data (2026-07-15)	✅ Done
8	Validate spend totals	✅ Done
9	Check missing tags by service	✅ Done
10	Report findings to Neeraja	👉 Next
11	Load historical data	👉 Pending
12	Create ADO Story	👉 Pending

9. Summary to Report to Neeraja
"GCP tables and SP created successfully. Loaded 2026-07-15 data — 125,478 raw rows collapsed to 97,673 Silver rows. Total spend matches exactly at $110,203.50. Tag coverage: 79% have App ID, 97% have Resource ID. Main gaps are GCP Compute Engine (11,473 missing App ID) and 60% missing region which is expected for BigQuery and serverless services. GCP has less tagging than Azure as expected — recommend proceeding with current implementation and investigating Compute Engine tagging separately."

10. ADO User Story
Title: Create GCP Aggregated Daily Resource Cost Table and Stored Procedure

User Story:
As a Data Engineer, I want to create a staging table, final table, and stored procedure for GCP daily resource cost aggregation in the Silver layer, So that GCP cloud spend data is available in a consistent, aggregated format — matching the pattern already established for Azure.

Acceptance Criteria:
●Investigate GCP source data in [Cloudability].[Daily_Spend] where vendor = GCP to identify correct column mappings
●Create staging table [Silver].[Cloudability_Daily_Resource_Cost_GCP_Staging]
●Create main table [Silver].[Cloudability_Daily_Resource_Cost_GCP]
●Create stored procedure [Silver].[usp_CloudabilityAggregate_DailySpend_GCP]
●Validate row counts and spend totals against source for GCP
●SP filters on vendor = GCP and runs for DATEADD(DAY, -3, GETDATE())
●Check tag coverage and document what is missing
●Unit tested with at least one GCP billing date

Notes:
●Parent story: Azure Aggregated Daily Resource Cost (already completed)
●GCP has no resource group — gcp_project mapped from vendor_account_name
●GCP has less tagging than Azure — some NULLs expected
●DECIMAL(38,6) and DECIMAL(38,10) used for large GCP quantity values
●HEAP + ROUND_ROBIN distribution required for NVARCHAR(MAX) columns in Synapse

Story Points: 5   |   Priority: Medium   |   Parent: Azure Cloudability Story