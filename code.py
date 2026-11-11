For the final Free SKU report, use the actual verified SKU from SQL wherever available.

For App Service resources:
- Include confirmed Free/F1 resources in the main Free SKU report.
- If SQL confirms a paid SKU, exclude that resource from the main Free SKU report and place it in a separate `Paid_SKU_Review` worksheet.
- If SQL cannot confirm the SKU, place the resource in `Unverified_SKU` with its original Aptio F1 description.
- Do not automatically assign F1 to every Aptio App Service record.

Preserve all 2,781 Aptio records and 129 additional SQL candidates across the main report and review worksheets, without losing or duplicating resources.

Use the team's exact 11-column format for the final Free SKU worksheets.

Do not overwrite the original Excel files. Produce the final workbook with a summary of confirmed Free, paid, and unverified resource counts.