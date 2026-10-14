Continue our MongoDB FOCUS POC using the existing September 2026 files and comparison script.

We found an unexplained $1,894.35 difference between Native FOCUS BilledCost ($4,420.14) and regular invoice Amount excluding credit rows ($6,314.49).

**Your next task is to investigate this difference using a read-only, local analysis.**

1. Compare Native FOCUS and regular invoice rows using the same organization, usage dates, SKU, project/account, region, and any other reliable matching fields.
2. Identify which invoice charges are missing from FOCUS or have different amounts.
3. Check whether differences come from discounts, units, pricing, currency, date handling, or other billing categories.
4. Verify whether the 420 credit rows were correctly excluded for the usage-only comparison.
5. Show daily and SKU-level differences, including the largest contributors to the $1,894.35 gap.
6. Do not assume that similarly named fields have identical billing meanings.
7. Do not invent explanations for unmatched charges.
8. Update the existing comparison Excel file with a new sheet called `Cost_Reconciliation`.
9. Update `Comparison_Summary.md` with confirmed findings and remaining unresolved differences.

Keep all original files unchanged. Do not call APIs, connect to databases, upload data, or modify production code. Do not expose sensitive organization or project identifiers in the summary.

Run the analysis locally and give me a simple explanation of the results.