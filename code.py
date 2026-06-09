Completed the simulated metrics implementation. 

Generated projected CPU, memory, and connection 
utilization based on right-sizing recommendations. 
CurrentEfficiency shows actual usage on the current 
SKU and WithinEfficiency shows the projected usage 
after the recommended SKU change — giving a clear 
before vs after comparison.

Results are stored in 
[Metrics].[MongoDBRightsizingRecommendations] 
across 284 clusters. No production data was affected.