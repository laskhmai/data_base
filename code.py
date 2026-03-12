f the Key Vault must remain RBAC-enabled and the modules cannot be modified, then the only safe option on our side would be to adjust the template configuration so the AppConfig module does not attempt to create access policies.

Currently encryption is enabled in the template, which triggers the module to look up the Key Vault and create azurerm_key_vault_access_policy. Since the KV is RBAC-enabled, that logic conflicts.

So my suggestion would be either:

Disable the encryption flag in the template so the module skips the Key Vault access policy logic, or

Use a Key Vault that supports access policies if encryption is required.

Otherwise the module would need to be updated to support RBAC with role assignments instead of access policies.




Since the Key Vault must remain RBAC-enabled and we cannot modify the module, one possible workaround would be to set `enable-encryption = false` in the template. This would prevent the module from attempting the Key Vault lookup and creating `azurerm_key_vault_access_policy`, which is currently causing the Invalid index error.
