Hi Sathish,
I reviewed the failure in detail and identified the root cause.

The pipeline is failing during terraform init because it is trying to access the Terraform Cloud organization "humanaprd", but the workflow login step is using the TFC_TOKEN_NPE token.

Since TFC_TOKEN_NPE is a non-prod token, it does not have permission to access the humanaprd (prod) organization. That’s why Terraform Cloud is returning the “unauthorized” error while reading the organization during backend initialization.

So this is not a Redis issue or Terraform code issue — it’s an org/token mismatch between the backend configuration and the workflow login credentials.

We need to align one of the following:

If DEV/QA/UAT should use a non-prod Terraform Cloud org, then we should update workspace.tf to reference the correct NPE organization instead of humanaprd.

If this workflow must access humanaprd, then the login step should use a token that has access to that org (e.g., TFC_TOKEN instead of TFC_TOKEN_NPE).

Once the organization and token are aligned correctly, the pipeline should proceed without the unauthorized error.

🔎 If He Asks “Why Did It Work Before?”

You can say:

It worked previously because the dev workflow was aligned with the correct workspace and token. After the backend changes (or when merging into main), the organization reference changed to humanaprd, but the workflow is still logging in using the NPE token, which caused the mismatch.