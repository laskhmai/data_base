 name: Delete Existing VM
      run: |
        az login --service-principal \
          -u ${{ secrets.SPN_CLIENT_ID }} \
          -p ${{ secrets.SPN_CLIENT_SECRET }} \
          --tenant ${{ secrets.SPN_TENANT_ID }}
        az vm delete \
          --resource-group sandbox-adfpoc-eastus2-sbx-rg \
          --name winvmasashir-vm-1 \
          --yes || true