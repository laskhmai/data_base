Hi,

The TFE_NPE_APPLY pipeline failed due to 3 main issues:

1. SDM token is missing/expired in TFE NPE workspace — needs to be renewed
2. Namespace variables (plainidpdp_base_namespace & azcli_base_namespace) are empty — need values added in TFE
3. Several Azure resources exist but aren't in Terraform state — need terraform import run

Can someone from the team please action these? Happy to jump on a call to walk through the details.

Thanks!