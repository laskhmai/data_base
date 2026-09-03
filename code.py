# --- SQL ARM deployments ---
import {
  to = module.win-appservice["appservice3"].azurerm_resource_group_template_deployment.site-extensions[0]
  id = "/subscriptions/40cf9c44-5c6e-48e5-870e-07cfc097bc87/resourceGroups/sfvbp-sfcloud-eastus2-prd-rg/providers/Microsoft.Resources/deployments/sfbluedmnsvc-claimexpinqy-ext"
}

import {
  to = module.win-appservice["appservice12"].azurerm_resource_group_template_deployment.site-extensions[0]
  id = "/subscriptions/40cf9c44-5c6e-48e5-870e-07cfc097bc87/resourceGroups/sfvbp-sfcloud-eastus2-prd-rg/providers/Microsoft.Resources/deployments/sfbluedmnsvc-ledger-ext"
}

# --- CyberArk target sets ---
import {
  to = module.windowsvm["windowsvm-adf-dm-stg"].module.se-cyberark-secret[0].idsec_sia_workspaces_target_set.targets["10.84.113.24"]
  id = "10.84.113.24"
}

import {
  to = module.windowsvm["windowsvm-adf-balances-bp-stg"].module.se-cyberark-secret[0].idsec_sia_workspaces_target_set.targets["10.84.113.23"]
  id = "10.84.113.23"
}

import {
  to = module.windowsvm["windowsvm-adf-dp-stg"].module.se-cyberark-secret[0].idsec_sia_workspaces_target_set.targets["10.84.113.22"]
  id = "10.84.113.22"
}

import {
  to = module.windowsvm["windowsvm-adf-claims-dm-stg"].module.se-cyberark-secret[0].idsec_sia_workspaces_target_set.targets["10.84.113.25"]
  id = "10.84.113.25"
}

import {
  to = module.windowsvm["windowsvm-adf-cp-dm-stg"].module.se-cyberark-secret[0].idsec_sia_workspaces_target_set.targets["10.84.113.26"]
  id = "10.84.113.26"
}