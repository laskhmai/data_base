
- name: Remove Tainted Site Extension From State
  working-directory: ./
  run: |
    terraform state rm 'module.func-padm-splunk-integration.azurerm_resource_group_template_deployment.site-extensions[0]'
