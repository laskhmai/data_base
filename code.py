Please identify all suspected fallback resources using the final gold table.

A suspected fallback resource is a record where Action = 'RightSize' but OverallWithinFamily/recommended SKU is null, blank or '-', especially when DtuRec and StgRec are also blank and savings are zero.

Separate the results into:

1. Valid RightSize — recommended SKU is populated and differs from CurrentSku.
2. Suspected fallback RightSize — recommended SKU is missing.
3. Other incomplete or inconsistent records.

Provide counts by month and the list of affected resource IDs. Then trace each suspected fallback resource through the recommendation logic and classify the failure reason, such as:

- All rows removed by null filtering.
- DTU recommendation returned None.
- Storage recommendation returned None.
- No qualifying SKU found.
- Insufficient rolling-window data.
- Clustering or seasonality logic produced no result.
- SKU metadata join failed.
- Another condition.

Do not change the code or data. Use read-only queries and analysis, and show evidence for each classification.