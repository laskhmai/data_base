In `FreeSku_F0_F1_Prod_QA_Stage.xlsx`, some resources have `AccountName = orphaned`.

Please investigate these using their full normalized Resource IDs.

1. Extract the Subscription ID from each Resource ID.
2. Check available SQL subscription/account mapping tables and existing Aptio account data.
3. Identify the actual AccountName for each subscription.
4. Update only `orphaned` values when a reliable mapping is found.
5. If no mapping exists, preserve `orphaned` and list the resources separately for review.
6. Do not guess the AccountName from the resource name or group name.
7. Keep the existing 11-column format, F0/F1 filters, and all resource records.
8. Save the corrected report as a new Excel file.

Please report how many `orphaned` account names were resolved and which source table provided each mapping.