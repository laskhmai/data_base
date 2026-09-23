Hi Po, please follow these steps to provision the Azure Windows VM in your new repo:

Step 1: Use the provided Azure Windows VM sample template (version 3.0.7) and provision/copy the sample template into your new repo.

Step 2: You don't need to modify the other template files. Update the required VM values in local.tf based on your requirement, such as VM size, image, availability zone, computer name and other required configuration.

Step 3: Make sure the appropriate TFC workspace is available/configured. The required secrets and credentials will be handled through the existing TFC/workflow setup, so don't add secrets directly into the code.

Step 4: The repo already has separate workflow YAML files for Dev, NPE and Prod, with Plan and Apply workflows.

Step 5: Based on where you want to provision the VM, trigger the appropriate Plan workflow. For example, for Dev, run the Dev Plan workflow; for NPE or Prod, select the respective workflow.

Step 6: Once the Plan completes successfully, review the Terraform plan and confirm the expected VM/resources are showing.

Step 7: Then trigger the corresponding Apply workflow for the same environment. This will provision the Azure Windows VM.

If you have any questions or face any issues while provisioning, please ping me.