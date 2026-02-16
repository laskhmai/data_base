In the current module, Terraform is not creating or managing subnets directly.
However, the storage account network rules and private endpoints explicitly reference a defined list of subnet IDs.

If a subnet is created manually via the Azure Portal and its subnet ID is not included in the Terraform configuration (for example in usr-custom-vnet-subnet-ids), Terraform will not delete the subnet itself.

However, during terraform apply, it will remove that subnet from the storage account network rules if it is not defined in code.

To retain access, the subnet ID must be added to the Terraform configuration.