In `FreeSku_Information_Updated_Final.xlsx`, some Environment values are showing `needs update`.

Please investigate those resources using their normalized Resource IDs and check the `environment` column in `[Gold].[AzureActiveResourceOwnerShipNormalized]`, with other authorized Azure inventory tables as fallback.

Fill the Environment with the actual verified value, such as DEV, QA, NPE, or PROD. Also check reliable environment tags where available.

Do not guess based only on the resource name or account name. Do not overwrite existing valid Environment values.

If the environment cannot be verified, leave it blank and list those resources in `Validation_Detail`.

Preserve the existing 11-column format and all resource records. Save the corrected workbook as a new file and provide the number of Environment values updated.