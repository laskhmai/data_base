Title: Implement Azure Metrics Collection in Databricks for Cloud 3.0

Description:
As part of the Cloud 3.0 migration, implement and validate the Azure metrics collection code in Databricks.

The code should use the metrics_config configuration to identify the required Azure resource types, metric names, aggregation, granularity, and collection window. It should collect metrics from Azure Monitor and load the data into the Databricks Bronze metrics table.

The implementation should also support pipeline run logging, retry handling, parallel metric collection, and validation of the collected row counts.

Acceptance Criteria:

* Read active metric configurations from metrics_config.
* Fetch required metrics from Azure Monitor API.
* Support multiple Azure resource types.
* Write collected metrics into the Bronze metrics Delta table.
* Log run status, collected rows, written rows, and failures.
* Support retries for failed API calls.
* Validate that metric data is collected correctly.
* No credentials or secrets should be hardcoded in the code.