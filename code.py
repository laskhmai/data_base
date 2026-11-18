Good. Now take one Azure SQL DB resource and one metric, for example cpu_percent / Average, and compare the API-fetched data against the existing Metrics.SQLDB_CPUAvg_5M table for the same resource and same time window.

Please compare timestamp, value, id/key, aggregation and row count, and tell me whether they match.

Keep everything strictly read-only. Do not insert, update or delete anything.