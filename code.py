i Sourya,

Thanks for sharing the workspace details. I checked the error — the workspace is accessible, but the failure is happening because Terraform is trying to fetch a TFC team (AZ_CId3_LOB_MLP_florenceai_PRD) which is not found in the .

Since this is a new tenant onboarding, it looks like the team might not be created/onboarded yet in Terraform Cloud.