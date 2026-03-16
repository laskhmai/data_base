Hi Juzer, good morning. Since our Terraform structure expects both NPE and PROD components to follow the same template, we can keep the existing logic and handle this in `locals`. If the storage account is required only in NPE, we can define it under `local.npe.storage` and keep the PROD section empty, like:

prd = {
storage = {}
}

This way, when the NPE workflow runs it will create the storage account, and when the PROD workflow runs no resource will be created.
