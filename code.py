Hi,

Here is a quick summary of what the Terraform plan will do when applied:

- Old private endpoints (file + queue) across storage1–storage7 will be deleted and new ones recreated under the updated module structure. Blob endpoints are also being added as new.
- Storage accounts will have sftp_enabled set to true, cross_tenant_replication set to false, and blob last_access_time disabled.
- New diagnostic settings (blob, queue, table) will be created for each storage.
- Local user (usr1) will be added to each storage.
- Function apps will restart due to AppInsights and Dynatrace settings updates.

Note: There will be a brief connectivity interruption to storage during the private endpoint recreation. Recommend applying during a maintenance window.

Please confirm if this is okay to proceed.