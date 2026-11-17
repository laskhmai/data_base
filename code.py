DB connectivity is confirmed.

Now continue READ-ONLY only.

Do not create another Python file unless necessary. Update/reuse the current
fetch-only work where possible.

Next I only want to verify the inputs required by metrics_sql.py.

1. Read the enabled configuration from Metrics.Settings for:
   ServiceType = 'Microsoft.Sql/servers/databases'

2. Show only the relevant columns:
   MetricName
   Aggregation
   Interval
   Days
   BackPeriod
   Destination
   Arguments

3. Read only 1-3 sample SQL Database resources from AZURE.vw_DBInventory
   where dbtype = 'SqlDb'.

Show:
   id
   id_key

4. Do not write anything.
5. Do not execute any stored procedure.
6. Do not upload anything.
7. Do not call Azure Metrics API yet.

Print the results and explain what each field means.

After that STOP and wait for me.