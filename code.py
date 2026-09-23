We have a user requirement to disable public disk access and enable private disk access for their VMs.

I noticed they are currently using a very old template version (13.0.4), while our newer template/module version 14.2 supports the required private disk access functionality.

Since their repo is quite old, moving directly to 14.2 may require a significant migration and could introduce breaking changes.

Do you have any suggestions on how we should approach this? Is upgrading to 14.2 mandatory to meet this requirement, or is there a supported way to enable private disk access/disable public disk access while keeping their existing template version?
