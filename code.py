Continue the MongoDB FOCUS POC using the existing September 2026 Native FOCUS CSV, Regular Invoice CSV, MongoDB_FOCUS_Comparison.xlsx, and Comparison_Summary.md.

The cost reconciliation is complete. Our next task is to identify which columns are important for Cloud 3.0 and which columns can be excluded.

**Tasks:**

1. Review all 49 Native FOCUS columns and all 40 Regular Invoice columns.
2. For each column, identify its purpose, whether it contains useful data, and whether it duplicates another field.
3. Classify each column as KEEP, OPTIONAL, REMOVE, or NEEDS VALIDATION.
4. Prioritize daily cost reporting, resource identification, organization/project identification, service, SKU/meter, region, usage, discounts, tags, and billing accuracy.
5. Compare the recommended fields against the existing Azure FOCUS staging schema (`Staging.Cost_focus`) and its 14 columns.
6. Identify fields missing from MongoDB Native FOCUS and fields MongoDB provides that could add value beyond the Azure schema.
7. Do not assume similar field names mean the same thing. Explain any mapping uncertainty.
8. Recommend a minimal set of columns for our MongoDB POC and explain why each is needed.
9. Add a new sheet named `Final_Column_Selection` to the existing comparison Excel workbook.
10. Update `Comparison_Summary.md` with the column recommendations and remaining open questions.

Use only local files. Keep original CSVs unchanged. No APIs, database connections, production changes, or uploads. Do not expose sensitive organization or project identifiers.

Explain the findings in simple English, including a final recommendation for our Cloud 3.0 MongoDB approach.