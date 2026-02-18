Hi,

Here’s what happened with the storage account.

The DEV pipeline ran correctly with environment = DEV.
But in storage.tf the condition is written like this:

contains(["npe","dev","qa"], var.env) ? local.qa.storage : local.prd.storage

Because dev is included in that list, Terraform is routing DEV to the QA storage configuration.

So when the plan ran in DEV:

It picked QA storage config

The existing DEV storage didn’t match QA config

Terraform marked it as “must be replaced”

That’s why it planned destroy/recreate

Terraform didn’t randomly delete anything — it followed the condition in the code.