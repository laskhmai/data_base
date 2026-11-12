Please update my previous Free SKU Excel report containing 58 resources.

Use `FreeSku_F0_F1_Free_Prod_QA_Stage.xlsx` as the base workbook.

I need ONLY TWO additional columns:

1. `Resource Creation Date`
2. `Confidence Level` (High / Medium / Low)

**Important instructions:**

1. Keep all existing 58 resources unchanged.
2. Preserve the original 11 columns, values, formatting, filters, and worksheet names (`Prod` and `QA-Stage`).
3. Add the two new columns at the end, after `tags`.
4. For Resource Creation Date, use only verified Azure creation timestamps identified in our previous SQL analysis.
5. Leave Resource Creation Date blank if no reliable Azure creation timestamp exists.
6. For Confidence Level, use the existing validation findings:
   - **High:** Current SQL inventory confirms the Free SKU and environment, without conflicting evidence.
   - **Medium:** SQL confirms a Free SKU, but evidence is historical, inactive, or otherwise not current.
   - **Low:** Free SKU is supported only by Aptio descriptions or free-tier flags, without reliable SQL confirmation.
7. If SQL sources disagree about the SKU, set confidence to Low and do not silently resolve the conflict.
8. Do not classify historical Free SKU evidence as High.
9. Do not add any other columns, validation status fields, or comments to the main sheets.
10. Do not remove, duplicate, reorder, or reclassify any resource.
11. Do not alter existing `sku_name`, `Environment`, ownership, or tags values.
12. Do not modify SQL tables.

Use the existing `Validation_Detail` and previous resource creation-date findings to populate these columns by normalized Resource ID.

Create a new workbook:

`FreeSku_Report_With_Date_Confidence.xlsx`

Verify that all 58 resources are preserved and that only these two columns were added to the main report.

Finally, provide the counts of High, Medium, and Low confidence resources, and how many reliable creation dates were populated.

Do not overwrite the original Excel file.