We need to perform a complete, evidence-based investigation of the SQL DTU recommendation issue.

## Primary problem

For August 2026, almost every resource received `Action = RightSize`, while the recommended SKU fields were blank and savings were zero.

The current action distribution is:

* January: 456 Optimal, 131 RightSize, 568 Terminate
* June: 425 Optimal, 107 RightSize, 662 Terminate
* July: 391 Optimal, 104 RightSize, 699 Terminate, 9 No Metrics
* August: 0 Optimal, 1,232 RightSize, 0 Terminate, 1 No Metrics

We currently suspect two related problems:

1. An existing fallback defect converts a blank or failed recommendation into `RightSize`.
2. An August-specific problem caused recommendation generation to fail for nearly every resource.

Please investigate both problems separately and then explain how they are connected.

## Important restrictions

* Do not modify any code, database records, tables, stored procedures or configuration.
* Use read-only queries and static code analysis only.
* Do not expose or repeat any credentials.
* Do not implement a fix yet.
* Do not introduce new Action values without confirming the downstream data contract.
* Clearly separate confirmed evidence, reasonable hypotheses and unresolved questions.

## Phase 1: Confirm the business rules and output contract

Identify from the code and available documentation:

* Conditions that produce `Optimal`.
* Conditions that produce `RightSize`.
* Conditions that produce `Terminate`.
* Conditions that produce `No Metrics`.
* Expected behavior when recommendation calculation returns `None`, blank or no qualifying SKU.
* Whether a valid RightSize record is allowed to have a blank recommended SKU.
* Required target-table fields for each Action.
* All valid values accepted by the downstream consumer.
* Whether `No Metrics` is the existing approved value for a calculation failure.
* Whether any other approved status exists for an uncalculated recommendation.

If the downstream contract is not available, document the exact questions that must be answered by the table owner or downstream team. Do not assume or invent accepted values.

## Phase 2: Confirm the final target-table facts

Use the live gold table:

`[Metrics].[SQLDbDTURecommendationEfficiencySavings]`

Validate the action distribution by month.

For every month, report:

* Total records and distinct resources.
* Count and percentage for each Action.
* RightSize records with a populated recommended SKU.
* RightSize records with a null, blank or `-` recommended SKU.
* Records with blank `DtuRec`.
* Records with blank `StgRec`.
* Records with blank `OverallWithinFamily`.
* Records with zero/null `WithinFamilySavings`.
* Records with zero/null `OutsideFamilySavings`.
* Records with blank efficiency fields.

Separate records into:

1. Valid RightSize: recommended SKU is populated and different from CurrentSku.
2. Suspected fallback RightSize: recommended SKU is null, blank or `-`.
3. Other incomplete or inconsistent records.

Provide the affected resource IDs and counts by month.

## Phase 3: Create controlled comparison groups

Select at least the following examples:

1. The same resource that produced a valid recommendation in July but fallback RightSize in August.
2. A historical resource with a genuine RightSize recommendation and populated recommended SKU.
3. A resource classified as Optimal.
4. A resource classified as Terminate.
5. A resource classified as No Metrics.
6. A resource affected by null `StorageMax`.
7. If available, any resource that produced a valid recommendation in August.

Use the same resource across July and August wherever possible. This is important because it reduces unrelated differences.

For each sample, capture:

* Resource ID and name.
* Current SKU.
* Month, DayType and HourType.
* Source-row count.
* Date range.
* DTU values.
* Storage values.
* Recommended SKU.
* Final Action.
* Spend30days.
* Savings.
* Efficiency fields.

## Phase 4: Compare July and August source data

Use `[Metrics].[SqlDataBasesAggregatedHourly]` and relevant source tables.

For the selected resources and for the overall monthly population, compare July and August:

* Total rows and distinct resources.
* Minimum and maximum timestamps.
* Expected versus actual hourly coverage.
* Missing days or weeks.
* Duplicate records.
* Null count and percentage for every required metric.
* Zero count and percentage for DTU metrics.
* Null and zero counts for storage metrics, including `StorageMax`.
* Current SKU distribution.
* DayType and HourType distribution.
* Business-hours and non-business-hours coverage.
* Inventory metadata join coverage.
* SKU configuration join coverage.
* Daily spend join coverage.
* Resources appearing in July but not August and vice versa.
* Schema, datatype or column-name changes between months.

The presence of August rows does not automatically prove that every required metric and date window is valid. Verify field-level completeness.

## Phase 5: Trace the complete source-to-target pipeline

Map and validate the full pipeline:

`Source tables → notebook transformations → gold-table insert → Stored Procedure 1 → intermediate metrics table → Stored Procedure 2 → final gold-table update`

Document:

* Each source and target object.
* Table grain and primary matching keys.
* Input/output columns.
* Filters.
* Joins and join types.
* Transformations.
* Default values.
* Row counts before and after every step.
* Fields inserted or updated at each step.

Known components include:

* `[Metrics].[SqlDataBasesAggregatedHourly]`
* `[Analytics].[AzureSqlDbDtuMetaConfig]`
* Inventory/archive source
* Daily-spend source
* SQL DTU RightSizing notebook and `query()`
* `[Metrics].[SQLDbDTURecommendationEfficiencySavings]`
* `usp_SqlDBDtuRightsizingSimulatedMetrics`
* Intermediate current/simulated metrics table
* `usp_SqlDBDtuRightsizingEfficiency`

Confirm the exact object names from the code/database.

## Phase 6: Trace row counts and values inside the notebook

For each controlled sample, record row counts and key values after every stage:

1. Raw source query.
2. Month/date filtering.
3. Business-hours/DayType/HourType filtering.
4. Current-SKU split.
5. Null-data cleaning.
6. SKU metadata join.
7. CPU/DTU input preparation.
8. Storage input preparation.
9. `recommendations()` execution.
10. `Stgrecommendations()` execution.
11. DTU recommendation output.
12. Storage recommendation output.
13. Combined recommendation selection.
14. `withinFamily` and `outsideFamily` assignment.
15. Action assignment.
16. Savings calculation.
17. Final cleanup/default-value replacement.
18. Gold-table insertion.

At each stage answer:

* How many rows entered?
* How many rows remained?
* Which filter or join removed rows?
* Which required values became null or blank?
* Which function returned `None`, blank or an empty DataFrame?
* Why was no qualifying SKU found?
* What exact condition assigned the final Action?

Pay special attention to blanket filtering such as:

`dataframe.notna().all(axis=1)`

Determine whether a null in an unrelated field such as `StorageMax` incorrectly removes rows needed for a DTU recommendation. Quantify how many resources and rows are affected by each filter.

## Phase 7: Investigate the August-specific failure

Trace the recommendation internals to determine why August produced almost no usable recommendations.

Review and compare July versus August for:

* Rolling-window start/end dates.
* Latest-month logic.
* Month ordering.
* String-to-date conversions.
* Inclusive/exclusive date boundaries.
* Minimum number of days/weeks required.
* Partial-month detection.
* Weekly aggregation.
* Clustering input size.
* K-means cluster assignment.
* Number of unique data points.
* Threshold calculations.
* Seasonality comparison logic.
* Trend direction.
* Business-hours filtering.
* Zero-value handling.
* Null-value handling.
* Recently processed month behavior.
* Any dependency on previous or future periods.
* Any hardcoded dates or month indexes.
* Any exception handling that silently returns `None`.
* Any differences in parameters or configuration.

For the same resource, show the exact inputs, intermediate outputs and branch decisions for July and August. Identify the first line or condition where their processing paths diverge.

Do not conclude that the source feed is good merely because rows exist. Do not conclude that source data is bad without field-level evidence.

## Phase 8: Confirm the fallback defect

Verify the suspected fallback path:

1. Recommended SKU variable is initialized to blank.
2. Recommendation functions fail to return a valid SKU.
3. The variable remains blank.
4. Blank recommended SKU is compared with `ActualSku`.
5. Because blank does not equal the current SKU, Action becomes `RightSize`.
6. Savings calculation cannot run without a valid recommended SKU.
7. Blank/null savings are later converted to zero.

Quantify this across all months:

* Total RightSize records.
* RightSize records with a blank recommended SKU.
* Percentage of RightSize records likely produced by fallback.
* Historical impact by month.
* August impact.
* Whether any other Action is affected by the same default/fallback behavior.

Distinguish the fallback path from the loop itself. Identify the exact lines and conditions involved.

## Phase 9: Validate downstream stored procedures separately

For the selected resources, verify values:

1. Immediately after the notebook inserts into the gold table.
2. After `usp_SqlDBDtuRightsizingSimulatedMetrics`.
3. In the intermediate simulated-metrics table.
4. After `usp_SqlDBDtuRightsizingEfficiency`.
5. In the final gold-table state.

Confirm:

* Whether both procedures ran for August.
* Their execution order.
* Join success rates.
* Rows unmatched during joins.
* Whether casing or whitespace in resource IDs affects matches.
* Whether `LOWER()` is applied consistently.
* Which fields each procedure updates.
* Whether the procedures merely propagate blank recommendations or create new blanks.

Keep this separate from the original Action-classification problem. Determine whether procedure execution explains only blank efficiency fields or also affects recommendation/savings values.

## Phase 10: Produce a root-cause matrix

For every suspected fallback resource, classify the failure reason where possible:

* Rows removed by null filtering.
* DTU recommendation returned `None`.
* Storage recommendation returned `None`.
* Both recommendation functions returned `None`.
* No qualifying SKU found.
* SKU metadata join failed.
* Inventory join failed.
* Spend join failed.
* Insufficient rolling-window data.
* Clustering returned no usable result.
* Seasonality logic returned no usable result.
* Date/month boundary issue.
* Stored-procedure join failed.
* Stored procedure did not execute.
* Other identified reason.
* Still unresolved.

Provide counts and percentages for each category. A single resource may have multiple contributing conditions, but identify the first failure point and primary cause.

## Evidence required before proposing a fix

Do not propose or implement a final fix until the following evidence is available:

1. July-versus-August comparison for the same resources.
2. Exact function and condition returning blank or `None`.
3. Exact explanation for why August behaves differently.
4. Confirmed business meaning of a failed recommendation.
5. Confirmed downstream accepted Action values.
6. Impact count for current and historical records.
7. Confirmation of whether efficiency procedures ran.
8. Identification of records requiring correction or backfill.
9. Regression-test expectations for previous months.
10. Evidence distinguishing valid RightSize from fallback RightSize.

## Expected resolution structure

After completing the investigation, recommend—but do not implement—separate fixes:

### Fix A: Classification/fallback defect

Prevent a null, blank or failed recommendation from automatically becoming `RightSize`.

Explain the correct decision structure using only approved Action values.

### Fix B: August-specific recommendation failure

Correct the confirmed rolling-window, clustering, seasonality, date-filtering, null-handling or other logic that caused August recommendations to fail.

Do not guess which component is responsible. Support the conclusion with July-versus-August evidence.

### Fix C: Data-quality safeguards

Recommend validation controls such as:

* Required-field checks.
* Row-count checks.
* Null-percentage thresholds.
* Join-coverage checks.
* Alert when one Action suddenly represents nearly all resources.
* Alert when recommended SKU is blank for a RightSize record.
* Alert when savings are zero despite nonzero spend and a requested SKU change.
* Explicit reason codes for recommendation failures.

### Fix D: Downstream procedure reliability

If applicable, recommend monitoring for stored-procedure execution, row-match rates and efficiency-field completion.

### Fix E: Historical correction

Determine which previous and August records were incorrectly classified and whether they need to be recalculated or backfilled.

## Final deliverable

Provide a structured report containing:

1. Executive summary.
2. Confirmed facts.
3. Complete data lineage.
4. Monthly impact analysis.
5. Valid versus fallback RightSize counts.
6. July-versus-August controlled comparisons.
7. Row-count and value trace.
8. Exact first failure point.
9. Confirmed fallback mechanism.
10. August-specific root cause, if proven.
11. Downstream stored-procedure findings.
12. Root-cause matrix.
13. Business/data-contract questions still requiring confirmation.
14. Proposed fixes separated by defect.
15. Validation and regression-test plan.
16. Historical backfill impact.
17. Remaining unknowns.

For every conclusion, include the supporting query result, code reference or data evidence. If something cannot be confirmed, label it as unresolved rather than making an assumption.

Begin with the controlled comparison of the same resources in July and August, then proceed through the phases in order.
