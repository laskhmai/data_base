We are building a right-sizing system for 
MongoDB clusters to help reduce cloud costs.

WHAT WE ARE DOING:

Step 1 — Collect hourly metrics
We collect CPU, Memory, Network and 
Connection metrics from all MongoDB 
clusters every hour and store them 
in an aggregation table.

Step 2 — Build recommendations
Using those hourly metrics we determine 
if each cluster is:
  - Too big (wasting money) → Scale Down
  - Right size → Keep
  - Too small (at risk) → Scale Up

Step 3 — Schedule daily
The process runs automatically every 
day in Azure Automation so recommendations 
are always up to date.

WHY WE ARE DOING IT:
To identify MongoDB clusters that are 
over or under provisioned so the team 
can take action to optimize cloud costs.

✅ Hourly metrics are collected for all 
   MongoDB clusters across 25 orgs

✅ Aggregation table stores one row 
   per process per hour

✅ Right-sizing recommendations are 
   generated for each cluster

✅ Each recommendation shows:
   - Current size (M10, M20, M30 etc)
   - Recommended size
   - Reason (CPU too low, Memory too high etc)

✅ No duplicate records created 
   when proc runs daily

✅ Code reads keys from Azure Key Vault
   not from a local file

✅ Logs show success/failure per org
   with org name and process count