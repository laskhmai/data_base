I want you to create a README file in this repository that acts as the permanent context/documentation for our Cloudability → FOCUS POC.

First, inspect the complete repository and all relevant Cloudability code that you have access to.

Document the current architecture from top to bottom, including:
- Cloudability API
- Python collectors for Spend, Properties, Categories and Tags
- CSV generation
- ADLS paths
- External tables
- Staging tables
- Stored procedures
- Cloudability Spend/Properties/Categories/Tags tables
- Daily_Spend_Aggregate
- Cloudability.Daily_Spend final table

For every step, document the source → processing → destination and mention the actual file/script/table/procedure names you find.

Also include a section called "Unknown / To Be Verified" for anything we still cannot prove, especially:
- What loads Staging.Spend, Staging.Properties, Staging.Categories and Staging.Tags
- Synapse/ADF pipeline or job names
- Triggers/schedules
- External-table orchestration
- Any other missing part of the end-to-end flow

Add another section called "FOCUS POC".

For now, explain only the known/proposed high-level direction:
Azure Cost Management → FOCUS-format data → Storage → POC processing → comparison with Cloudability.Daily_Spend.

Do not invent FOCUS mappings until we receive and inspect the actual FOCUS dataset.

Also create a Mermaid architecture diagram showing the current confirmed Cloudability flow and the proposed FOCUS POC flow.

IMPORTANT:
This README should become the source of truth for this project.
Whenever we discover something new in future sessions, update this same README.
Before doing future Cloudability/FOCUS analysis, read this README first so we do not restart the investigation from the beginning.

Clearly label information as:
- CONFIRMED
- TO BE VERIFIED
- PROPOSED

Use evidence from the actual code/database information available to you. Do not guess.

Do not change any application code, SQL objects, pipelines, configuration, or production data. Documentation only.

After creating the README, show me the file path and summarize:
1. What you documented
2. What is confirmed
3. What is still unknown
4. What we should investigate next