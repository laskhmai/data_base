Currently the slot resource does not manage app_settings. So Terraform will not update FUNCTIONS_WORKER_RUNTIME for the slot.

To update it properly, we would need to modify the module and add app_settings to the azurerm_windows_function_app_slot resource. That would be the correct long-term fix.

Alternatively, it can be updated manually in Azure portal as a temporary solution.

For Question 2:

If FUNCTIONS_WORKER_RUNTIME is deleted manually from the slot in Azure portal, it will not affect Terraform state, since the slot app settings are not currently managed by Terraform.