Hi Neeraja garu,

Researching memory metrics for MongoDB.

Current situation:
  We store MEMORY_RESIDENT in MB
  Convert to % using SKU RAM from MetaConfig
  This works but is indirect

Direct 0-100% metrics available:

1. CACHE_FILL_RATIO
   WiredTiger cache utilization %
   Most relevant for MongoDB performance
   Cache full = data spilling to disk

2. SYSTEM_MEMORY_PERCENT_USED
   System-level memory %
   Need to verify this exists in Atlas API

Checking from Postman now to confirm
which metrics return 0-100 values.

Will share findings shortly!

Thank you!