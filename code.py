I need to validate your findings manually in the database. You already have the notebook, scripts, table names, column names and full investigation context.

Please prepare a complete SQL validation script using the exact schema, table and column names found in the code. Do not use placeholders unless a required object genuinely cannot be identified.

## Objective

We need to manually confirm this suspected failure chain:

1. `StorageMax` became increasingly null and reached 100% null in August.
2. The notebook’s blanket `notna().all(axis=1)` filter removes rows when `StorageMax` is null.
3. This leaves no input for DTU and storage recommendation functions.
4. The recommended SKU remains blank.
5. The fallback logic incorrectly assigns `Action = 'RightSize'`.
6. Savings remain blank and are later converted to zero.

## Requirements

* Generate read-only `SELECT` queries only.
* Do not execute updates, inserts, deletes, stored procedures or code changes.
* Use the exact database object and column names from the notebook and stored procedures.
* Do not include or expose credentials.
* Put the queries in the correct execution order.
* Add a short comment above every query explaining what it validates.
* After every query, explain how to interpret the result.
* Clearly state the expected result based on the current investigation.
* If any expected result differs from the actual output, explain what that difference would mean.
* Make each query independently executable where possible.

## Query 1: Confirm source-table schema

Provide a query to list the columns and datatypes in:

`[Metrics].[SqlDataBasesAggregatedHourly]`

Identify the exact:

* Timestamp/date column.
* Resource ID column.
* Resource-name column.
* Current-SKU column.
* DTU columns.
* `StorageMax` column.
* DayType and HourType columns.

## Query 2: Monthly source-data completeness

For every available month, return:

* Total rows.
* Distinct resources.
* Minimum and maximum timestamps.
* `StorageMax` null rows.
* `StorageMax` populated rows.
* `StorageMax` null percentage.
* Number of resources where `StorageMax` is null for 100% of their rows.
* Number of resources where `StorageMax` is populated for 100% of their rows.
* Number of resources with partially populated `StorageMax`.

This query should confirm or disprove the reported progression:

* June: approximately 34% null
* July: approximately 76% null
* August: 100% null

## Query 3: August StorageMax validation

For August 2026, return:

* Total rows.
* Distinct resources.
* Null and populated `StorageMax` counts.
* Null percentage.
* Minimum, maximum and average non-null `StorageMax`.

The current expected result is approximately 783,601 rows, around 843 resources and 100% null `StorageMax`. Use the query output as the authority if the counts differ.

## Query 4: StorageMax by resource

For every August resource, return:

* Resource ID.
* Resource name.
* Current SKU.
* Total rows.
* Null `StorageMax` rows.
* Populated `StorageMax` rows.
* Null percentage.
* Minimum and maximum `StorageMax`.
* Minimum and maximum timestamp.

Order the results so resources with 100% null values appear first.

## Query 5: DTU data health

For July and August, return monthly and resource-level checks for every DTU column used by the recommendation code:

* Total rows.
* Null rows.
* Zero rows.
* Populated nonzero rows.
* Null percentage.
* Zero percentage.
* Minimum, maximum and average values.

This must verify whether DTU data remained generally available while `StorageMax` failed.

## Query 6: Same-resource July-versus-August comparison

Identify resources present in both July and August where:

* July has a populated `OverallWithinFamily` recommendation.
* August has a blank/null/`-` recommendation.
* August has `Action = 'RightSize'`.

Return the number of such resources and detailed rows containing:

* Resource ID and name.
* Month.
* DayType and HourType.
* Current SKU.
* DtuRec.
* StgRec.
* OverallWithinFamily.
* OverallOutsideFamily.
* Action.
* Spend30days.
* WithinFamilySavings.
* OutsideFamilySavings.

Also provide the source metrics for those same resources by month.

## Query 7: Validate the anchor resource

Use the exact resource ID for the previously analyzed anchor resource, such as `lenticular_dev`, if confirmed from the existing results.

Compare July and August:

* Source-row counts.
* DTU metrics.
* `StorageMax` null counts and percentage.
* Current SKU.
* Recommended SKU.
* Action.
* Spend.
* Savings.
* Efficiency fields.

Do the same for the original `csarch-sqldb-dev` resource.

## Query 8: Monthly Action distribution

From `[Metrics].[SQLDbDTURecommendationEfficiencySavings]`, return the Action distribution by month:

* Total rows.
* Distinct resources.
* Optimal count and percentage.
* RightSize count and percentage.
* Terminate count and percentage.
* No Metrics count and percentage.
* Any null or unexpected Action values.

Confirm or disprove the known January, June, July and August distributions.

## Query 9: Separate valid and fallback RightSize records

Classify every RightSize record into:

### Valid RightSize

* Recommended SKU is populated.
* Recommended SKU is different from CurrentSku.

### Suspected fallback RightSize

* Recommended SKU is null, blank or `-`.
* DtuRec and/or StgRec is missing.
* Savings is null or zero.

Return by month:

* Total RightSize records.
* Valid RightSize count.
* Suspected fallback RightSize count.
* Fallback percentage.

Also provide the detailed August fallback-resource list.

## Query 10: Validate blank recommendation and zero-savings relationship

For every Action value, return:

* Total records.
* Blank recommended-SKU count and percentage.
* Blank DTU recommendation count.
* Blank storage recommendation count.
* Zero/null within-family savings count.
* Zero/null outside-family savings count.
* Nonzero Spend30days with zero savings count.

This must distinguish expected blank values for `Terminate` or `Optimal` from abnormal blank values for `RightSize`.

## Query 11: Validate No Metrics behavior

Identify all `No Metrics` resources and compare them with fallback RightSize resources.

For both groups, return:

* Raw source-row count.
* Rows remaining after relevant source filters, if reproducible in SQL.
* DTU field availability.
* Storage field availability.
* Recommended fields.
* Final Action.

The goal is to prove the difference between:

* Truly no raw source rows.
* Raw rows exist, but later filtering would remove them.

## Query 12: Reproduce the blanket null filter in SQL

Using the exact columns selected into the notebook dataframe, reproduce the effect of:

`dataframe.notna().all(axis=1)`

For July and August, return:

* Raw row count.
* Rows passing the all-columns-non-null condition.
* Rows removed.
* Removal percentage.
* Removal reason by nullable column where possible.

Also create separate counts showing:

* Rows valid for DTU recommendation based only on required DTU columns.
* Rows valid for storage recommendation based only on required storage columns.
* Rows removed only because `StorageMax` is null.

This must demonstrate whether valid DTU rows are being removed because of missing storage data.

## Query 13: SKU metadata join coverage

Using `[Analytics].[AzureSqlDbDtuMetaConfig]`, verify:

* Every CurrentSku in July and August has a matching SKU configuration.
* Same-family candidate SKUs exist.
* Required DTU, storage and cost fields are populated.
* Number of unmatched resources.
* Number of resources with no qualifying candidate SKU.
* Possible casing or whitespace differences in SKU and InstanceType values.

## Query 14: Inventory and spend join coverage

Validate the inventory/archive and Daily_Spend joins:

* Total source resources.
* Matched resources.
* Unmatched resources.
* Duplicate matches.
* Blank AppId/environment values.
* Missing Spend30days.
* Resource-ID casing or whitespace mismatches.

Provide the unmatched resource details.

## Query 15: Downstream stored-procedure validation

Without executing the procedures, provide queries to verify:

* Whether August rows exist in the current/simulated metrics intermediate table.
* Row counts by month.
* Match rate between the gold table and intermediate table.
* Missing joins by resource, Month, DayType and HourType.
* Blank CurrentEfficiency, WithinFamilyEfficiency and OutsideFamilyEfficiency.
* Potential InstanceType casing/whitespace mismatches.
* Whether August efficiency fields were updated.

Keep these findings separate from the RightSize-classification defect.

## Query 16: Historical impact

Across every available month, return:

* Resources with `StorageMax` null for all rows.
* Resources with partial `StorageMax` nulls.
* RightSize records with blank recommended SKU.
* Resources with nonzero spend but zero savings.
* Resources whose source data existed but recommendation calculation appears to have failed.

This will identify the likely historical impact and backfill scope.

## Query 17: Data-quality reconciliation summary

Create one final monthly summary containing:

* Source rows.
* Distinct resources.
* `StorageMax` null percentage.
* Resources with 100% null `StorageMax`.
* Rows surviving the notebook-equivalent null filter.
* Optimal count.
* Valid RightSize count.
* Fallback RightSize count.
* Terminate count.
* No Metrics count.
* Blank recommended-SKU count.
* Zero-savings count.
* Missing efficiency count.

## Output format

Provide:

1. One complete SQL script in execution order.
2. A numbered explanation matching each query.
3. Expected results based on current evidence.
4. A checklist where I can record whether each result matched.
5. A final decision table explaining what each possible result means.
6. A list of any checks that cannot be performed because the required schema, table, column or procedure information is unavailable.

Do not make code changes or propose the final fix in this response. The goal is to give me exact read-only SQL queries that I can run manually and use to confirm the complete root cause.
