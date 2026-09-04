cd /path/to/your/repo
terraform init
terraform state rm 'module.db-server["azuresql-sf-prov-stage"].azurerm_resource_group_template_deployment.automatic-tuning-arm-template'
terraform state rm 'module.db-server["azuresql-sf-prov-stage"].azurerm_resource_group_template_deployment.security-alerts-arm-template[0]'
