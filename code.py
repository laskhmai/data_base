Hi Sonu, thanks for checking with **13.0.18**.

Yes, the earlier issue was due to **module 13.0.11 using the old `log` block**, which is not compatible with the AzureRM **v4 provider** used in the pipeline. In **13.0.18 this was updated to `enabled_log`, so the plan runs successfully now.**

I noticed the plan is showing **2 resources to add** (`vnet-integration` and `linux-webapp-private-endpoint`). Since we didn’t add new code on our side, it’s likely Terraform is detecting them due to **module version/state differences** after switching to 13.0.18.

Before applying, it would be good to verify whether those resources already exist in Azure or if they are missing from the Terraform state, just to avoid creating duplicates.
