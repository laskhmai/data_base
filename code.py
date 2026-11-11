## Task: Discover Azure Free SKU Data Across Hybrid Synapse SQL Databases

### Objective

I have an Excel report exported from Aptio Cloudability containing Azure resources associated with Free SKU offerings.

I want to identify whether our existing Hybrid Synapse / SQL environment already stores similar information.

**Important: Do not search only for column names. Search the actual data values inside tables as well.**

### 1. Discover relevant databases and tables

Connect to the authorized Hybrid Synapse SQL environment.

Inspect the databases and schemas available to my account, including:

- `analytics`
- `Gold`
- `Silver`
- `Staging`
- `Cloudability`
- Other accessible schemas containing Azure inventory, cost, usage, billing, resource properties, or SKU information

First inspect database metadata to identify potentially relevant tables.

Prioritize tables related to:

- Azure resource inventory
- Azure resource ownership
- Cloudability Spend
- Cloudability Properties
- Cloudability Tags
- FOCUS cost and usage
- Azure resource metadata
- Product information
- Billing meters
- Resource SKU information

### 2. Search actual values, not just column names

Search the actual data stored in relevant text, JSON, and descriptive columns.

Look for values containing terms such as:

`Free`, `Free Tier`, `Free Plan`, `F1`, `Express edition`, `Free SKU`, `App Service Free`, `Cosmos DB Free`.

Also look for values similar to these Aptio Item Descriptions:

- `Azure App Service Free Plan - F1`
- `Azure App Service Free Plan - Linux - F1`
- `Azure Cosmos DB - Free Tier`
- `Microsoft Defender for Storage - Free`
- `Azure Arc-enabled SQL Server - Express edition`

The matching terms may appear inside columns named:

- `ItemDescription`
- `ProductName`
- `Sku`
- `SkuName`
- `Properties`
- `ResourceProperties`
- `MeterName`
- `ServiceName`
- `Operation`
- `InstanceType`

However, do not restrict the search to these names. Inspect other relevant text fields where the data could be stored.

### 3. Search JSON fields

Some tables may contain Azure resource metadata stored as JSON.

For example:

```json
{
  "sku": {
    "name": "F1",
    "tier": "Free"
  }
}
```

Search JSON fields such as `Properties` for SKU values, tier information, or Free plan descriptions.

Use the appropriate JSON functions supported by the SQL engine.

Do not assume all text fields contain valid JSON.

### 4. Search efficiently and safely

This is a production-connected SQL environment.

- Use only SELECT queries.
- Start with system metadata and relevant schemas.
- Avoid scanning every column of every large table.
- Use targeted searches and bounded samples first.
- Check table sizes before expensive queries.
- Do not create tables or modify database records.
- Do not execute unbounded full-table searches without reviewing their impact.
- If a query requires scanning a very large dataset, show me the proposed query first.

### 5. Identify which tables contain matching data

For every table where relevant values are found, provide:

| Database | Schema | Table | Column | Example matching value | Match count |
|---|---|---|---|---|---|

Also identify whether the matching table contains:

- Resource ID
- Resource Name
- Resource Group
- Subscription / Account
- SKU Name
- SKU Tier
- Product or Item Description

### 6. Check resource-level matching

Compare the Free SKU records found in SQL with my Aptio Excel report.

Use normalized Azure Resource IDs to identify matching resources.

If Resource IDs contain additional prefixes, extract the full Azure path starting from `/subscriptions/`.

Do not use partial resource names or approximate matches.

Determine:

1. Which Aptio Free SKU resources also exist in SQL.
2. Which SQL tables contain the corresponding Free SKU information.
3. Which resources have SKU information in SQL but not in Aptio.
4. Which Aptio resources have no matching SQL SKU information.

### 7. Validate what "Free" represents

Distinguish between:

- Confirmed Azure Free SKU
- Free usage tier or allowance
- SQL Server Express free edition
- Free billing meter or promotion
- Zero-cost usage record

Do not classify a resource as having a Free SKU simply because a text value contains `Free` or its cost is zero.

### 8. Deliver results before modifying Excel

First provide a discovery report containing:

1. Databases and tables inspected.
2. Tables where Free SKU-related data was found.
3. Exact column names containing those values.
4. Sample matching records.
5. Total matching records and distinct resources, where practical.
6. Whether each table can identify the actual resource.
7. Which SQL table is best suited for enriching my existing Excel report.
8. Which Free SKU categories are supported by the data.

**Do not update the Excel file yet.**

Once the discovery is completed, recommend the best table or combination of tables for populating SKU information in the Excel report.

Do not assume a table has no Free SKU data merely because its column names do not contain SKU or Item Description.