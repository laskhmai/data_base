We are working on a MongoDB FOCUS POC for Cloud 3.0.

I have downloaded two September 2026 billing files:

1. MongoDB Native FOCUS CSV
2. MongoDB Regular Invoice API CSV

Both files are available locally in my Mongo_FOCUS folder.

**IMPORTANT RULES:**
- Do not modify the original CSV files.
- Do not connect to MongoDB APIs, SQL Server, Azure, or any production system.
- Do not upload data anywhere.
- All processing must happen locally using Python.
- Do not print sensitive organization IDs, project IDs, credentials, or tags.
- Use one Python script rather than multiple scripts.

**STEP 1 — Inspect both files**

Find the two CSV files in the workspace and identify their structures.

The regular invoice CSV may contain billing metadata before the actual column headers.

Report:
- Total rows and columns
- Available fields
- Billing period
- Minimum and maximum usage dates
- Missing or empty columns

**STEP 2 — Compare the cost data**

For both files, compare September 2026 data using matching usage dates and the same organization.

Check:
- Daily cost totals
- Monthly cost totals
- Native FOCUS BilledCost and EffectiveCost
- Regular invoice Amount
- Cost differences and percentage differences
- Possible reasons for mismatches

Do not assume Amount and BilledCost are equivalent without validation. Preserve full precision during calculations.

**STEP 3 — Compare all columns**

Identify:
- Fields available in both files
- Fields available only in Native FOCUS
- Fields available only in the regular invoice
- Missing FOCUS fields
- Fields containing useful additional information
- Duplicate or overlapping descriptions
- ChargeCategory values, including whether they contain only Usage

Compare against our existing SQL staging requirements:

ChargePeriodStart, SubAccountId, SubAccountName, ResourceId, ResourceType, ServiceName, RegionName, BilledCost, EffectiveCost, ChargeCategory, PricingCategory, SkuMeter, ServiceCategory, ServiceSubcategory.

Also evaluate useful additional fields such as BillingPeriodStart, BillingPeriodEnd, ChargePeriodEnd, BillingCurrency, ResourceName, and Tags.

Do not invent mappings for unavailable fields.

**STEP 4 — Create a comparison report**

Create one Python script:

`scripts/compare_mongodb_focus.py`

Generate:

`output/MongoDB_FOCUS_Comparison.xlsx`

Excel sheets:
1. Summary
2. Daily_Cost_Comparison
3. Column_Mapping
4. Data_Quality
5. Recommended_Fields

Also create:

`output/Comparison_Summary.md`

**STEP 5 — Final recommendation**

Based on the actual data, explain:
- Which API provides more useful information?
- Do daily and monthly costs match?
- Which columns should we retain?
- Which columns can we exclude?
- Which approach requires less transformation?
- What still needs to be tested for daily automation?

Important: These September files cannot prove whether MongoDB Native FOCUS reports are updated daily. Mark daily API freshness as requiring a separate test.

**Execution instructions:**

First inspect the files and explain their structures. Then create and run the Python script locally, validate the output, and summarize the results.

Do not make any production changes.