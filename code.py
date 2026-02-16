In the current module, Terraform is not creating or managing subnets directly.
However, the storage account network rules and private endpoints explicitly reference a defined list of subnet IDs.

If a subnet is created manually via the Azure Portal and its subnet ID is not included in the Terraform configuration (for example in usr-custom-vnet-subnet-ids), Terraform will not delete the subnet itself.

However, during terraform apply, it will remove that subnet from the storage account network rules if it is not defined in code.

To retain access, the subnet ID must be added to the Terraform configuration.


In the current module design, there isn’t another supported way to retain the subnet access without including it in Terraform.

Since the storage account network rules enforce an explicit list of subnet IDs, any subnet not defined in the configuration will be removed from the allowed list during terraform apply.

To ensure consistent state management and avoid configuration drift, the subnet ID needs to be added to the Terraform configuration.