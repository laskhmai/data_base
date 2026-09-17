Our primary issue is to determine why all resources are receiving the `RightSize` action and why the recommended SKU and savings fields are blank or zero. Please keep this as the main troubleshooting objective.

Start by mapping the complete end-to-end pipeline: source/base tables → notebook transformations → gold-table insertion → Stored Procedure 1 → intermediate metrics table → Stored Procedure 2 → final gold-table update.

At every stage, document the input and output tables, joins, filters, business rules, row counts, and important field values. Specifically trace where the `Action` value is assigned as `RightSize`, where the recommended SKU is calculated, and where savings are calculated or updated.

Do not analyze only one resource. Compare:

1. One affected resource showing `RightSize` with blank SKU or zero savings.
2. One correctly processed resource, if available.
3. The overall number of resources receiving each action.

Confirm whether all resources are genuinely receiving `RightSize` by querying the final target table. Then determine whether the issue originates in the source data, the notebook’s null filtering and fallback logic, the stored-procedure joins, or the downstream updates.

Do not modify any code or data yet. First provide the complete evidence-based root-cause analysis for the original `RightSize` issue.
