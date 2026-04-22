Hi Bhupendra,

This is NOT a Terraform issue. Terraform is working correctly:
✅ VM created successfully
✅ Extension triggered correctly

The issue is with the gatewayInstall.ps1 script itself - it is failing during gateway installation (exit code 1).

This could be due to a stale gateway file from previous failed attempt. We need to check the logs on the VM at:
C:\WindowsAzure\Logs\Plugins\Microsoft.Compute.CustomScriptExtension\tracelog.log

Can we get RDP access via StrongDM to check?