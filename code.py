# Task: Final Free SKU Resource Validation and Creation Date Enrichment

## Business Requirement — Mohan

Mohan has requested two things:

1. **Validate the accuracy of the Free SKU resource report.** Confirm whether the listed Azure resources actually have Free SKUs (F0, F1, or Free), and identify any discrepancies between Aptio Cloudability and Hybrid Synapse SQL.
2. **Identify when each resource was originally created in Azure.** Add a new column called `Resource Creation Date` to the existing Excel report.

We need to complete this using our existing Hybrid Synapse SQL environment because I do not have Azure Portal access.

## 1. Input Excel

Use the latest report:

`FreeSku_F0_F1_Free_Prod_QA_Stage.xlsx`

It contains **58 unique resource records**:

- Production: 42
- QA: 15
- Stage: 1

Preserve these 58 records during validation. Do not remove records automatically.

The current report follows the team's original 11-column format:

1. Resourcetype
2. Resourcename
3. Resourcegroupname
4. AccountName
5. sku_name
6. Environment
7. Application_name
8. Application_Owner
9. Application_ID
10. BillingOwnerAppsvcid
11. tags

**Add one new column at the end:**

12. `Resource Creation Date`

Do not change the original column names or their order.

## 2. Identify the resources

Recover the original full Azure Resource IDs from `Validation_Detail` or the previously used input files.

Normalize Resource IDs by removing only any extra prefix before `/subscriptions/`.

Match resources using the complete normalized Azure Resource ID.

Do not match using Resource Name alone.

Confirm that all 58 Excel records have a unique and correctly identified Azure resource.

## 3. Search for actual resource creation dates

Check the available Hybrid Synapse inventory tables, especially:

- `[Gold].[AzureActiveResourceOwnerShipNormalized]`
- `[AZURE].[Resources]`
- Relevant active and inactive Gold resource inventory tables
- Other Azure inventory or metadata tables identified during discovery

Search for resource creation dates in both ordinary columns and JSON data.

### A. Search tags

Inspect the actual values of the `tags` column.

Look for keys such as:

- `CreatedDate`
- `CreationDate`
- `CreatedAt`
- `CreatedOn`
- `ResourceCreatedDate`
- `creation_time`
- `created_time`
- `createdBy`

Do not assume these exact keys exist. Discover the actual keys in the data, including different capitalization.

A `createdBy` value is not a creation date.

### B. Search Properties JSON

Check columns such as:

- `Properties`
- `properties`
- `ResourceProperties`
- `systemData`

Look for:

- `systemData.createdAt`
- `properties.createdTime`
- `createdTime`
- `createdAt`
- Other ARM creation timestamp fields

Use appropriate JSON parsing functions supported by the SQL engine.

Handle invalid or missing JSON safely.

### C. Check inventory creation-date columns

Inspect actual columns such as:

- `CreatedDate`
- `CreationDate`
- `resource_created_date`
- `created_at`
- `createdTime`

Before using a field, determine whether it represents:

1. Actual Azure resource creation time
2. Custom tag creation date
3. Resource first observed by the inventory pipeline
4. Database record creation date
5. Billing first-seen date

**Do not treat pipeline ingestion dates or first billing dates as actual Azure resource creation dates.**

## 4. Compare date sources

For each resource, retrieve all available creation-date candidates.

Use this evidence priority:

1. Azure ARM `systemData.createdAt` or service-specific ARM creation timestamp
2. Other verified Azure resource creation timestamp
3. Custom creation-date tag, only if its meaning has been validated

If a reliable Azure creation date is found, populate `Resource Creation Date`.

If only an unverified tag, pipeline date, or billing first-seen date is available, leave `Resource Creation Date` blank and document the candidate separately.

Do not guess missing dates.

Preserve the date's original timezone and precision, or clearly document any conversion.

## 5. Validate the Free SKU accuracy

For each of the 58 resources, verify its SKU using existing SQL inventory.

Check:

- `[Gold].[AzureActiveResourceOwnerShipNormalized]` — `Sku`
- `[AZURE].[Resources]` — `Sku`
- `[ASTRA].[AppSvcInventory]` — `skuSize`
- `[Analytics].[AzureAppServicePlan]` — `AppServiceSku`

For Azure App Services, use the correct App Service Plan Resource ID rather than assuming the website's SKU is the plan SKU.

Confirm whether the SKU is:

- `F0`
- `F1`
- `Free`
- Paid
- Missing or conflicting

Search actual SKU values in JSON when necessary.

**Important:** Do not classify a resource as a confirmed Free SKU solely because the Aptio Item Description contains `Free Plan - F1`.

Compare Aptio evidence against the relevant SQL plan or resource configuration.

Also identify whether the SQL evidence is current or historical.

## 6. Validate resource status

For every resource, determine whether it is:

- Active
- Deleted / Inactive
- Unknown

Check current resource inventory and deletion indicators where available.

Do not automatically classify an unmatched resource as deleted.

Record the source table and last verification date used for this determination.

Because Mohan requested data accuracy, clearly distinguish resources that are currently active from resources found only in historical inventory.

## 7. Update Excel

Create a new workbook:

`FreeSku_Final_Validated_With_CreationDate.xlsx`

Keep the existing `Prod` and `QA-Stage` sheets.

Maintain the team's formatting:

- Original 11 columns unchanged
- New column L: `Resource Creation Date`
- Existing resource order preserved
- Existing SKU values preserved unless a correction is supported and separately documented
- No duplicate resources
- Excel filters enabled
- Date column formatted consistently

Preserve all 58 resource records.

Do not add validation labels to the main report's existing SKU column.

If any record contains incorrect or conflicting SKU information, document the finding in the validation worksheet instead of silently changing it.

## 8. Add Validation_Detail worksheet

For every resource, include:

- Resource ID
- Resource Name
- Resource Group
- Account Name
- Existing Excel SKU
- Aptio Item Description
- SQL SKU
- SKU Source Table
- SKU Validation Result
- Resource Status
- Resource Status Source
- Resource Creation Date
- Creation Date Source Table
- Creation Date Column / JSON Key
- Creation Date Confidence / Evidence
- Last Verified Date
- Remarks

Clearly distinguish verified creation dates from custom tags and first-seen dates.

## 9. Add Validation_Summary worksheet

Provide the following:

### Resource accuracy

- Total resources: 58
- SQL SKU-confirmed Free resources
- Paid SKU discrepancies
- SKU conflicts
- SKU information unavailable
- Active resources
- Inactive/deleted resources
- Resource status unknown

### Creation-date coverage

- Actual Azure creation dates found
- Creation dates recovered from Properties JSON
- Creation dates recovered from verified tags
- Resources with only pipeline first-seen dates
- Resources with only billing dates
- Resources with no reliable creation date

### Environment breakdown

- Production
- QA
- Stage

Include the exact count for each category and reconcile totals.

## 10. Final validation

Before completing:

1. Confirm that all 58 original resource records remain present.
2. Verify each Resource ID matches the correct Excel resource.
3. Verify that every populated creation date has a reliable source.
4. Confirm that no billing first-seen date was incorrectly reported as Azure creation date.
5. Confirm that paid SKU discrepancies remain visible in Validation_Detail.
6. Confirm that active and inactive resource counts are supported by inventory evidence.
7. Open the generated workbook using an Excel-compatible reader and validate the structure and data.
8. Do not modify any SQL tables.
9. Do not overwrite the original Excel file.

Use only read-only SQL queries.

## 11. Final response

After creating the Excel, provide:

- Final Excel file path
- Number of resources validated
- Number of actual Azure creation dates found
- Number of missing creation dates
- Number of active and inactive resources
- Number of SKU-confirmed Free resources
- Any important mismatches between Aptio and SQL

Also provide a short explanation I can share with Mohan about the accuracy and source of the report.

**Do not claim 100% accuracy unless every resource, SKU, deployment status, and creation date is independently verified.**