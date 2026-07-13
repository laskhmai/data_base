

Just wanted to share a quick update:

We have made the following changes to the recommendations logic:

1. Peak CPU check added — even if P95 is low, if raw CpuMax peak × 2 > 100% we mark it as NoChange (cluster could hit capacity after downsize)

2. Seasonality detection — using STL decomposition to identify repeating patterns (e.g. weekend batch jobs). Seasonal clusters get NoChange

3. Connections logic fixed — high connections now correctly blocks Downsize with comment "Review Connection Pooling"

4. Savings calculation improved — using actual billing rates from Spend table with provider-specific discount ratios (Azure ~32%, GCP ~29%)

Please review when you get a chance:
[Metrics].[MongoDBRightsizingRecommendations_STL]

One pending topic I wanted to discuss with you:

We have 307 out of 327 clusters with autoscaling enabled. These clusters have a configured minimum SKU (e.g. M50) but can scale up to a maximum SKU (e.g. M80) during high load.

Currently our recommendations are based on the configured minimum SKU. But the metrics are collected at the actual running SKU (which could be M80).