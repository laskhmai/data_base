From the screenshots I can partially infer the issue, although I don’t have full visibility since the run is being executed locally and I don’t have access to the environment.

The Key Vault TestKV-Glapi appears to be configured with the Azure RBAC permission model, which means access policies are not available. However, the AppConfig module attempts to create azurerm_key_vault_access_policy resources and references data.azurerm_key_vault.this[0].id.

Since the Key Vault is RBAC-enabled, the module may not be able to apply access policies as expected, which could cause the Key Vault lookup to return an empty collection and lead to the Invalid index error.