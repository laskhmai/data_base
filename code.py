## Task: Enrich Free SKU Excel Report Using SQL Data

I have an Excel report exported from Aptio Cloudability containing Azure resources with Free SKU information.

Some columns in Excel contain missing values, `(not set)`, `NULL`, or blanks.

I want you to compare this Excel file with the SQL table:

**Database:** `analytics`  
**Table:** `[Gold].[AzureActiveResourceOwnerShipNormalized]`

### Objective

Populate only the missing values in the existing Excel report using matching resource records from the SQL table.

**Do not overwrite existing valid values. Do not modify the SQL database.**

### Step 1: Inspect both data sources

1. Read the Excel file and identify all columns.
2. Inspect the SQL table schema to identify matching fields.
3. Identify columns containing missing values, including:
   - Resource Name
   - Resource Group Name
   - Account Name
   - Environment
   - Application Name
   - Application Owner
   - Application ID
   - Billing Owner App Service ID (`BillingOwnerAppsvcid`)
   - Other relevant ownership fields
4. Look for SQL fields such as `billing_owner_appsvcid`, `application_name`, `environment`, `Sku`, `ResourceGroupName`, `Properties`, and Resource ID.
5. Confirm the correct SQL column names before querying. Do not assume the names are identical to Excel.

### Step 2: Match records using Resource ID

Both Excel and SQL contain Resource IDs.

However, some Resource IDs contain an additional prefix or identifier at the beginning.

The actual Azure resource path should be the same.

For example:

Excel:

`123456_/subscriptions/abc/resourceGroups/rg-dev/providers/Microsoft.Web/sites/app01`

SQL:

`/subscriptions/abc/resourceGroups/rg-dev/providers/Microsoft.Web/sites/app01`

These should be treated as the same resource.

Implement the matching logic:

1. Normalize both Resource IDs.
2. Identify the `/subscriptions/` portion of the resource path and remove any prefix before it.
3. Normalize leading/trailing whitespace, slash separators, and case.
4. Compare the entire normalized Azure resource path.
5. Match using the longest exact normalized resource path, not merely a short substring.
6. Never match two resources solely because their resource names are the same.
7. If no `/subscriptions/` path exists, examine other reliable identifiers, but do not guess or force a match.

**Important:** Do not remove arbitrary characters from Resource IDs. Preserve the full Azure path beginning at `/subscriptions/`.

### Step 3: Retrieve missing values

For each Excel resource:

- Find the matching SQL record.
- Check which Excel columns contain missing values.
- Retrieve the corresponding values from SQL.
- Populate only those missing Excel cells.

Treat the following as missing:

- Empty cell
- NULL
- `(not set)`
- `N/A`
- `None`
- Whitespace-only values

Do not replace existing valid Excel data.

If SQL also contains a missing value, leave the Excel cell unchanged.

### Step 4: Handle duplicates and conflicting SQL records

The SQL table may contain multiple records for the same resource.

Check fields such as:

- `is_current`
- `last_verified_date`
- `last_modified_date`
- `mapping_created_date`

Prefer the current record with the latest reliable verification date.

If multiple records have conflicting ownership values, flag them for review rather than choosing arbitrarily.

Maintain one row per unique Resource Name + Resource Group Name + Account Name in the final report, provided the records also resolve to the same resource identity. Do not merge different resource types or subscriptions accidentally.

### Step 5: Preserve Free SKU information

The Excel report contains Free SKU resources identified from Aptio Item Description.

- Preserve existing Free SKU evidence.
- Check the SQL `Sku` column for additional SKU information.
- Do not overwrite an existing valid SKU without verification.
- Flag differences between Aptio SKU descriptions and SQL SKU values.

### Step 6: Generate an updated Excel file

Create a new Excel file:

`FreeSku_Information_Updated.xlsx`

Preserve the original Excel file unchanged.

Maintain the existing column order, worksheets, formatting, and resource records wherever possible.

Add an additional worksheet called `Validation_Summary` containing:

| Resource ID | Field Updated | Previous Value | New Value | SQL Source | Match Status |
|---|---|---|---|---|---|

Also provide:

- Total Excel resources
- Number successfully matched with SQL
- Number of cells updated
- Number of resources with no SQL match
- Number of ambiguous matches
- Number of conflicting records
- Number of missing fields that remain unresolved

### Safety and validation rules

- Execute only read-only SQL queries.
- Do not update, insert, delete, or modify SQL records.
- Do not change existing valid Excel data.
- Do not use approximate or fuzzy matching without explicit review.
- Do not fill missing values with guessed information.
- Do not overwrite the original Excel file.
- Verify that the number of Excel resource rows is unchanged unless duplicate removal is explicitly justified and reported.
- Show a preview of proposed changes and the matching results before writing the final output file.

First inspect the Excel columns and SQL schema, then implement the matching and enrichment process.