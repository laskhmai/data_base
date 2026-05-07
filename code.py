Hi Udaya,

Nate and I have reviewed this further. Here is what we found:

✅ Our ECA linuxvmscaleset module is working correctly — it is successfully provisioning the VMSS and the custom-script812.sh content IS being passed to the VM via the Custom Script Extension.

The ODM install failure is happening inside the script itself, not in our module. We found two blockers:

1. Artifactory 401 — When we tested the repo URL from the VM:

2. Zscaler blocking — The VM is also hitting 403 Forbidden from Zscaler when trying to reach external URLs. Please check if the VM subnet needs to be whitelisted in Zscaler for Artifactory access.

These two issues are outside our module — they need to be resolved on your end with the security/network team and whoever manages the Artifactory credentials.