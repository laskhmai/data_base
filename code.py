Please update my final Free SKU Excel report according to Neeraja's latest requirement.

She specifically wants only Free-tier SKUs (F0/F1) across Production, QA, and Stage environments.

1. Read the latest Free SKU workbook, including the FreeSku_Information and Unverified_SKU sheets where applicable.

2. Filter resources to include only:
   - SKU Name = F0 or F1
   - Environment = PROD, Production, QA, or Stage, including verified equivalent environment values.

3. For missing or `needs update` Environment values, use normalized Resource ID matching against `[Gold].[AzureActiveResourceOwnerShipNormalized]` and other authorized inventory sources to retrieve the correct environment.

4. Do not assume NPE, DEV, or sandbox means QA or Stage. Only include resources with a verified environment classification.

5. Do not classify a paid SKU as F0/F1 merely because Aptio Item Description contains a Free Plan description. Use the SQL SKU validation results.

6. Keep the exact 11-column structure and formatting of the team's original FreeSku_Information Excel.

7. Create separate sheets:
   - `Prod`
   - `QA-Stage`
   - `Summary`

8. Do not modify the original files. Create `FreeSku_F0_F1_Prod_QA_Stage.xlsx`.

9. In Summary, provide:
   - Total F0/F1 resources
   - Production count
   - QA count
   - Stage count
   - Excluded resource counts by reason

10. Preserve a separate audit of excluded and uncertain records so no source data is lost.

Please report the final unique resource count after filtering. Do not invent or force a count.