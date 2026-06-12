Hi Neeraja garu,

I've made the following updates based on your feedback:

1. True 95th Percentile (Aggregation table)
   Changed the P95 calculation to use true 
   cluster-level P95 instead of taking MAX of 
   individual process P95 values.

2. ProcessKey added (Aggregation table)
   Added MaxCpuProcessId and MaxMemProcessId 
   columns to track which process drove the 
   highest CPU and memory values.

3. Simulated Metrics table created
   [Metrics].[MongoDBRightsizingSimulatedMetrics]
   This table stores projected metrics showing 
   how the cluster would perform on the 
   recommended SKU.

4. Low-CPU SKU recommendation added
   RecommendedSku now shows two options like 
   "M50, M50-low-CPU". Low-CPU is only shown 
   when peak CPU is below 50%.

5. Efficiency stored procedures created
   usp_MongoDBRightsizingSimulatedMetrics
   usp_MongoDBRightsizingEfficiency
   These are called automatically after the 
   recommendations are generated.

Updated tables:
  [Metrics].[MongoDBRightsizingAggregated5Min]
  [Metrics].[MongoDBRightsizingRecommendations]
  [Metrics].[MongoDBRightsizingSimulatedMetrics]

Please review and let me know your thoughts.
Thank you!