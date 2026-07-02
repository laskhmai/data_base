Hi Pankaj,

I reviewed the latest terraform module `se-windowsvm-cloud-3-0` version `3.0.0`, specifically the VM and managed disk configuration.

The module currently creates/attaches OS disk and data disks, but I do not see explicit disk network access settings such as `network_access_policy`, `public_network_access_enabled`, `disk_access_id`, or `azurerm_disk_access`.

So based on the current module code, version 3.0.0 does not appear to explicitly cover the Prisma recommendation for limiting Azure VM disk network access. This may require a module enhancement or additional disk-level configuration as per the ticket recommendation.

Thanks.