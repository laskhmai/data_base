Please update the local output file paths in my duplicate notebook `SUBS_Cost_mgmt_Focus_SQL_Test.ipynb`.

The existing path `C:\Users\KAN2210\Desktop\Focus` belongs to my colleague, not me.

Please make the following changes:

1. Automatically detect my current Windows user profile using Python's `Path.home()`.
2. Create a `Focus` folder under my user profile if it doesn't already exist.
3. Update both `OUTPUT_FILE` and `SQL_OUTPUT_FILE` to save CSV files inside my local `Focus` folder.
4. Use separate filenames for the original 16-column CSV and SQL-compatible 14-column CSV.
5. Check the `CERT_PATH` and tell me whether it points to my colleague's folder. Do not change it without confirming the correct certificate location.
6. Keep the subscription configuration and all existing data processing logic unchanged.

**Important restrictions:**
- Modify only the duplicate notebook.
- Do not touch the original notebook.
- Do not modify or load anything into SQL Server.
- Do not upload anything to Azure Blob Storage.
- Do not execute the notebook or call Azure APIs.
- Do not display credentials or secrets.

After making the changes, show me the final local output paths and confirm both CSV files will be saved on my laptop.

Wait for my approval before running the notebook.