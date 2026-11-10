Correction to the Excel update:

The `BillingOwnerAppsvcid` column does not currently exist in the Excel file.

Please do the following:

1. Inspect `[Gold].[AzureActiveResourceOwnerShipNormalized]` and identify the exact SQL column for Billing Owner App Service ID, expected to be `billing_owner_appsvcid`.

2. Add a new column named `BillingOwnerAppsvcid` to the existing Excel report.

3. Match each Excel resource with SQL using the normalized full Azure Resource ID, removing only any prefix before `/subscriptions/`.

4. Populate the new `BillingOwnerAppsvcid` column with the corresponding SQL values for verified matches.

5. Leave the new cell blank if no SQL match exists or the SQL value is NULL.

6. Continue filling missing Application Name, Application Owner, and other previously approved fields only when the SQL mapping has been verified.

7. Do not replace `Application ID` with `billing_owner_appid`. Keep Application ID unchanged unless its correct mapping is independently verified.

8. Preserve all 2,781 original resource records, their existing valid values, and the Free SKU information.

9. Add a `Validation_Summary` worksheet showing the number of values populated in the new column, other missing values filled, unmatched resources, and unresolved fields.

10. Save the result as `FreeSku_Information_Updated.xlsx`. Do not overwrite the original Excel or modify SQL data.

Please show the final column mapping and update counts after generating the file.