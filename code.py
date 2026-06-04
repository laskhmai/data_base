Hi team, same approach as last time but a DIFFERENT target. This time clear soft_delete_enabled off the key vault resource, not the credential-key-vault data source. One-line steps (run on INT first, then Prod):

1. Find address: terraform state list | grep azurerm_key_vault   (pick the one WITHOUT data. prefix)
2. Get id: terraform state pull > state.json && grep -A2 soft_delete_enabled state.json   (grab the "id")
3. Remove from state: terraform state rm '<address>'
4. Re-import: terraform import '<address>' '<key vault id>'
5. Re-run the pipeline.

Also remove soft_delete_enabled from the .tf config.