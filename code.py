- name: Delete Existing VM
  run: |
    terraform init -reconfigure
    terraform destroy \
      -target='module.windowsvm["windowsvmnpe"].azurerm_windows_virtual_machine.this[0]' \
      -auto-approve || true