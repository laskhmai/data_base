terraform state rm 'data.vault_generic_secret.sdm_auth'
terraform state rm 'module.sdm-vault-auth.data.http.jwt'
terraform state rm 'module.sdm-vault-auth.data.http.token'

terraform state rm 'module.windowsvm["windowsvm-adf-bal-bp-dev"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-bal-bp-perf"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-bal-bp-sit"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-bal-bp-uat"].sdm_resource.this[0]'

terraform state rm 'module.windowsvm["windowsvm-adf-claims-bp-dev"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-claims-bp-perf"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-claims-bp-sit"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-claims-bp-uat"].sdm_resource.this[0]'

terraform state rm 'module.windowsvm["windowsvm-adf-claims-dm-dev"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-claims-dm-perf"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-claims-dm-sit"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-claims-dm-uat"].sdm_resource.this[0]'

terraform state rm 'module.windowsvm["windowsvm-adf-cp-bp-dev"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-cp-bp-perf"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-cp-bp-sit"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-cp-bp-uat"].sdm_resource.this[0]'

terraform state rm 'module.windowsvm["windowsvm-adf-cp-dm-dev"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-cp-dm-perf"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-cp-dm-sit"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-cp-dm-uat"].sdm_resource.this[0]'

terraform state rm 'module.windowsvm["windowsvm-adf-dm-uat1"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-dm-uatv2"].sdm_resource.this[0]'

terraform state rm 'module.windowsvm["windowsvm-adf-dp-dev"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-dp-sit"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-dp-uat1"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-dp-uatv2"].sdm_resource.this[0]'

terraform state rm 'module.windowsvm["windowsvm-adf-legacy-joborchestrator-dev"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-adf-legacy-joborchestrator-uat"].sdm_resource.this[0]'

terraform state rm 'module.windowsvm["windowsvm-npe"].sdm_resource.this[0]'
terraform state rm 'module.windowsvm["windowsvm-sit"].sdm_resource.this[0]'