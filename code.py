I want to preserve the original Azure FOCUS Python notebook exactly as it is.

Please follow these steps:

1. Locate the original notebook `SUBS_Cost_mgmt_Focus_runbook.ipynb`.
2. Create a duplicate copy in the same folder named `SUBS_Cost_mgmt_Focus_SQL_Test.ipynb`.
3. Do not modify, rename, delete, or overwrite the original notebook.
4. In the duplicate notebook only, prepare a separate SQL-compatible CSV output for the existing `Staging.Cost_focus` table.
5. Keep the existing 16-column FOCUS CSV output unchanged.
6. For the SQL-compatible CSV:
   - Rename `ConsumedService` to `ServiceName`.
   - Rename `ConsumedRegion` to `RegionName`.
   - Exclude `ResourceGroup` and `ProviderName`.
   - Match all 14 SQL table columns in their exact order.
7. Validate the date format, decimal precision, and ActualCost/AmortizedCost merge logic. Do not assume that matching column names guarantee correct FOCUS cost semantics.
8. Add validation checks for missing columns, duplicate rows, null values, and cost totals.
9. Save the SQL-compatible CSV with a different filename so it cannot overwrite the original output.

**Restrictions:**
- Do not modify the original notebook.
- Do not modify SQL table structures or records.
- Do not execute any API requests or database writes.
- Do not expose or copy plaintext secrets into new code. Replace any hardcoded credentials in the duplicate with references to approved secure configuration.
- Only modify the duplicate notebook.
- After completing the changes, show the differences between the original and duplicate notebooks and explain them.

I want to review the duplicate code before running it.