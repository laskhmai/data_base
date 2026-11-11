## Task: Create Separate Excel Report for 129 Additional Free SKU Resources

From your previous Hybrid Synapse SQL discovery, you identified **129 additional potential Free SKU resources** that were not present in my original Aptio Cloudability Excel report.

I want a separate Excel file containing **only those additional SQL resources**.

### Step 1: Retrieve the 129 resources

Use your previous discovery results and the SQL inventory tables, including:

- `Gold.AzureActiveResourceOwnerShipNormalized`
- `AZURE.Resources`
- Other relevant SQL inventory tables identified during discovery

Retrieve the 129 additional Free SKU candidates.

Compare them with my original Aptio Excel using normalized full Azure Resource IDs.

Remove any prefix before `/subscriptions/`, normalize the paths, and perform exact matching.

**Include only SQL resources that are not present in the original Aptio Excel.**

### Step 2: Create the Excel file

Create:

`SQL_Additional_129_FreeSKU_Review.xlsx`

Include these columns:

1. Resource Type
2. Resource Name
3. Resource Group Name
4. Account Name
5. Subscription ID
6. Resource ID
7. SKU Name
8. SKU Tier
9. Environment
10. Application Name
11. Application Owner
12. Application ID
13. BillingOwnerAppsvcid
14. Tags
15. Source SQL Table
16. Free SKU Evidence
17. Validation Status
18. Include in Final Report (leave blank)
19. Review Comments (leave blank)

### Step 3: Validate the 129 records

For every resource:

- Confirm its SKU information using the SQL inventory.
- Identify whether it is a true Free SKU or a free usage tier.
- Exclude records that are only zero-cost billing entries from the confirmed category.
- Remove duplicate Resource IDs.
- Confirm the resource does not already exist in the Aptio Excel.
- Do not invent missing values.

Classify each resource as:

- Confirmed Free SKU
- Free Tier / Allowance
- Needs Verification

### Step 4: Create a summary sheet

Add a worksheet called `Summary` showing:

- Total additional unique SQL resources
- Resource counts by Azure service
- Confirmed Free SKU count
- Free Tier / Allowance count
- Needs Verification count
- Count missing from original Aptio Excel

Your previous analysis reported 129 additional resources, but the displayed category counts summed to 133. Reconcile this difference.

If the verified unique count is not exactly 129, explain why and use the actual count rather than forcing the result to 129.

### Important instructions

- Do not modify my original Aptio Excel.
- Do not modify `FreeSku_Information_Updated.xlsx`.
- Do not add these resources to the final report yet.
- Do not modify any SQL tables.
- Execute only read-only SQL queries.
- Preserve the full Resource IDs for validation.
- Create only this separate Excel file and its summary.
- Make the output easy to review and filter.
- Do not include the original 2,781 Aptio resources.

After creating the file, provide its exact path, final unique resource count, and a breakdown by resource type.