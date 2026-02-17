Hi Nate,
Just to give context on the changes —

Satish made updates to the Redis Terraform configuration and also modified the GitHub workflows related to QA/UAT execution.

Specifically:
• Workflow changes were made to how QA and UAT environments are triggered (matrix/branch configuration was adjusted).
• There were changes around backend/workspace configuration and how workspace.tf gets created during runtime.
• Branch structure was also adjusted (feature → dev → main flow changed slightly).

After these updates, we are seeing inconsistent behavior between DEV vs QA/UAT pipelines. DEV worked correctly, but QA/UAT are either showing “No changes” or behaving unexpectedly.

Can you help review whether the workflow/workspace mapping changes could be causing this?