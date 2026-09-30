Now perform the same data-availability validation on Cloudability.Daily_Spend, using read-only SELECT queries only.
First confirm whether Cloudability.Daily_Spend contains data for the same Azure comparison window: 2025-06-26 through 2025-07-25.
If the complete window is not available, report the exact overlapping date range between dbo.ActualCost_RR and Cloudability.Daily_Spend. Do not silently substitute another period.
For the overlapping period, profile all 41 Cloudability.Daily_Spend columns and return:
Cloudability column | total rows | non-null/non-empty rows | populated % | 3 redacted sample values
Then add a second section containing only the candidate FOCUS-related fields from our structural matrix, showing:
FOCUS field | Cloudability candidate column | populated % | sample values | current mapping classification
Do not decide that a semantic mapping is confirmed merely because both columns contain data. This step is only data availability and overlap validation.
Finally summarize:
POPULATED (>=95%)
PARTIALLY POPULATED (>0% and <95%)
EMPTY (0%)
Also report:
- exact Cloudability date range
- exact Azure/Cloudability overlapping date range
- row count for that overlap
- distinct vendor values in the overlap
- distinct vendor_account_name count
Strictly read-only: SELECT/metadata only. No INSERT, UPDATE, DELETE, MERGE, TRUNCATE, CREATE, DROP, ALTER, EXEC, stored procedures, files, pipelines, or configuration changes.