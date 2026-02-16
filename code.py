have a clarification regarding the workspace configuration.
In workspace.tf, we currently reference:

workspaces {
  name = "Azure-Cld3-CWX-phmdds-me-DEV"
}

Do we have separate Terraform Cloud workspaces for QA and UAT as well (e.g., ...-QA, ...-UAT), or are we using a single shared NPE workspace for all non-prod environments?