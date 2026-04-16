# Upload gatewayInstall.ps1 to storage account for SHIR
resource "null_resource" "upload_gateway_script" {
  provisioner "local-exec" {
    command = <<EOT
      az storage blob upload \
        --account-name azstoragetest11201 \
        --container-name npeexamplecontainer1 \
        --name gatewayInstall.ps1 \
        --file ${path.module}/gatewayInstall.ps1 \
        --overwrite \
        --auth-mode login
    EOT
  }
  depends_on = [module.storage]
}