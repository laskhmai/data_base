resource "azurerm_storage_blob" "gateway_script" {
  name                   = "gatewayInstall.ps1"
  storage_account_name   = "azstoragetest11201"
  storage_container_name = "npeexamplecontainer1"
  type                   = "Block"
  source                 = "${path.module}/gatewayInstall.ps1"
}