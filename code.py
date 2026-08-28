#export TF_LOG="debug"
hostname
terraform version
terraform init
terraform state rm 'module.eca-linuxvmscaleset.azurerm_linux_virtual_machine_scale_set.main'
terraform import 'module.eca-linuxvmscaleset.azurerm_linux_virtual_machine_scale_set.main' /subscriptions/74c67909-4512-46cb-b973-83d19dee90f5/resourceGroups/seks-odm-ps-int-east2-rg/providers/Microsoft.Compute/virtualMachineScaleSets/dccc-seks-odm-res-int-August2026-950-v4
terraform show
terraform providers