Continue in strict READ-ONLY mode.
Before investigating cost semantics, validate whether the comparison grain itself is causing the difference.
For the same two POC dates, inspect both dbo.ActualCost_RR and Cloudability.Daily_Spend.
Group BOTH sides by:
Date + Subscription + normalized ResourceId
On ActualCost_RR:
- count source rows per resource/day
- SUM(Cost)
- SUM(Quantity)
On Cloudability.Daily_Spend:
- count rows per resource/day
- SUM(amortized_spend)
- SUM(usage_quantity)
Then compare the aggregated resource/day totals between the two tables.
Specifically show:
- how many resource/day groups have 1 row, 2 rows, 3 rows, 4+ rows on each side
- examples where the same resource/day has multiple rows
- whether aggregating all rows first materially reduces the Cost vs amortized_spend difference
- daily total before and after using the corrected aggregated grain
Do not assume Daily_Spend is already one row per resource/day. Prove its actual grain from the data.
Do not modify any database object or existing workbook yet. Show the evidence first.