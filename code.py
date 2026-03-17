Hi Nate,

I’m supporting a ticket where we hit an error with the Diagnostic Settings module. The pipeline is using AzureRM provider v4, and module version 13.0.11 was using the older `log` block, which caused the error. Sonu tested with version 13.0.18 and the plan now runs successfully.

However, the plan shows **2 resources to add**:
• `azurerm_resource_group_template_deployment.vnet-integration`
• `azurerm_private_endpoint.linux-webapp-private-endpoint`

Before applying, I wanted to check — if these resources already exist in the environment, should we **import them into Terraform state** instead of creating them again? Please let me know the best approach here.
