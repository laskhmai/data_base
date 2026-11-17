Good. The read-only discovery is confirmed.

Proceed to the FIRST Azure Metrics API proof-of-concept only.

Use ONE non-production Azure SQL Database resource from the sample you found
in AZURE.vw_DBInventory.

For the first test use:

MetricName: cpu_percent
Aggregation: Average

Use the ARM resource ID from the `id` column.

IMPORTANT:
Do not process all resources.
Do not process all metrics.

Use only one resource and one metric.

For this PoC, use a small time window if possible (for example the last
1-2 hours) rather than fetching the full 5-day production configuration.
This is only to prove authentication + API fetch + parsing works.

AUTHENTICATION:

Follow the same service-principal authentication pattern used by the legacy
metrics_sql.py:

AZURE_TENANT_ID
AZURE_CLIENT_ID
AZURE_CLIENT_SECRET

Do NOT hardcode any credentials.

Do NOT print:
- client secret
- access token
- authorization header

If I need to set the secret manually in my terminal, give me the command
structure with a placeholder only. Never ask me to paste the actual secret
into Claude.

Create/update the fetch-only implementation so that it does:

1. Authenticate to Azure.
2. Get an access token.
3. Use the selected resource ARM ID.
4. Call the Azure Monitor Metrics API.
5. Request only cpu_percent / Average.
6. Parse the returned timeseries.
7. Create a pandas DataFrame.
8. Print:

   - resource id (you may redact subscription portion in normal logs)
   - metric name
   - aggregation
   - DataFrame columns
   - DataFrame dtypes
   - number of rows
   - first 10 rows
   - null counts if useful

9. STOP.

DO NOT:

- INSERT into SQL/Synapse
- UPDATE anything
- create any table
- write into Databricks
- write Delta
- upload CSV
- upload to ADLS/Storage
- call cbtools.uploadData()
- modify the original metrics_sql.py

Use the existing fetch-only file we are building rather than creating many
new Python files.

Handle these errors clearly:

401 authentication/token issue
403 RBAC/permission issue
404 resource/metric issue
429 throttling
timeout/network issue
empty metric response

If the API response is empty, do NOT assume the code failed. Print the
response status/details safely and explain whether the metric may simply
not apply to that resource.

After the first API call and DataFrame output, STOP and show me the result.
Do not proceed to Bronze/Databricks writes yet.