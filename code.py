Hi Neeraja garu,

You are right — both metrics can exceed 100%.

MEMORY_RESIDENT:
  Stored in MB, converted using SKU RAM
  Can exceed 100% if processes 
  are summed incorrectly

SYSTEM_MEMORY_PERCENT_USED:
  Should be 0-100% per server
  But shows > 100% because same server
  memory is reported by multiple processes
  We are summing what should be MAX-ed

Fix: For all memory metrics use MAX 
across processes (not AVG or SUM)

MAX(SYSTEM_MEMORY_PERCENT_USED) per hour
= true server memory utilization ✅
= always 0-100% ✅

This would be the cleanest metric
for memory recommendations.

Shall we use this approach?

Thank you!