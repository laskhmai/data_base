I am working on the Azure FOCUS cost data pipeline.

**Python file:** `SUBS_Cost_mgmt_Focus_runbook.py`  
**SQL target table:** `Staging.Cost_Focus`

Please perform the following analysis:

1. Read the Python file and identify all columns generated in the final CSV output.
2. Connect to the existing authorized SQL Server database and retrieve the complete schema of `Staging.Cost_Focus`, including column names, data types, and column order.
3. Compare the CSV columns against the SQL table columns.
4. Provide a comparison table with:
   - CSV Column Name
   - SQL Table Column Name
   - Match Status
   - Required Change
5. Identify missing columns, extra columns, naming differences, and potential data type mismatches.
6. Check whether the Python script needs column renaming or additional transformations before loading the CSV into the table.
7. Recommend the exact Python code changes required to generate a CSV compatible with `Staging.Cost_Focus`.

**Important instructions:**
- Read-only investigation only.
- Do not execute the Python script or call external APIs.
- Do not modify Python files or SQL objects.
- Do not insert, update, or delete any database records.
- Do not display credentials or tokens.
- Do not assume that similarly named fields have identical meanings.
- If the SQL table cannot be located, report the connection/database limitation instead of guessing.

**Expected output:** A complete CSV-to-SQL column mapping table, identified mismatches, and proposed code changes for my review.