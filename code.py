Please update `FreeSku_Information_Final.xlsx`.

1. Keep the existing 255 Free SKU records unchanged.
2. Add the 154 records from `Unverified_SKU` below those 255 records in the `FreeSku_Information` sheet.
3. Maintain exactly the same 11 columns, order, and formatting as the team's sample.
4. For `sku_name`, use the short SKU value from the available description:
   - Free Plan – F1 → `F1`
   - Free Tier → `Free`
   - Free → `Free`
   - Use other SKU names only when supported by source data.
5. Do not add a Validation Status column or any unverified labels to the main sheet.
6. Preserve the resource names, resource groups, account names, ownership details, and tags.
7. Keep the 2,501 paid SKU records in their separate worksheet.
8. Do not duplicate resources or overwrite the original workbook.
9. Save as `FreeSku_Information_Updated_Final.xlsx`.

The main sheet should contain 409 records if all 154 additional records are distinct. Keep the existing `Validation_Detail` sheet so the original evidence remains available, without adding validation labels to the main report.