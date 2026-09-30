Using read-only SELECT queries only, compare Azure dbo.ActualCost_RR against Azure-only rows from Cloudability.Daily_Spend for the exact overlap period 2025-06-26 through 2025-07-25.
Do not create or modify anything.
First normalize join keys:
- ActualCost_RR.ResourceId is an ARM resource ID.
- Cloudability.Daily_Spend.resource_id may be in <GUID>::<ARM ID> format. Extract only the ARM ID portion when present.
- Normalize casing only if needed for comparison.
Then produce:
1. Daily aggregate comparison:
   date | ActualCost_RR total Cost | Cloudability total amortized_spend | absolute difference | percentage difference
2. Resource-level join statistics:
   - total Azure ActualCost_RR distinct resource IDs
   - total Cloudability Azure distinct parsed ARM resource IDs
   - matched resource IDs
   - unmatched on each side
   - match percentage
3. For 20 matched sample resources, show:
   date | resource_id | ActualCost_RR ResourceName | Cloudability Azure_Resource_Name | ActualCost_RR Product/ConsumedService | Cloudability service_name | ActualCost_RR ResourceLocation/MeterRegion | Cloudability region | ActualCost_RR Quantity | Cloudability usage_quantity | ActualCost_RR Cost | Cloudability amortized_spend
Redact sensitive identifiers where appropriate, but preserve enough structure to verify matching.
Do not claim Cost = EffectiveCost or amortized_spend = EffectiveCost. Treat this as source-to-source comparison only.
Finally summarize which fields appear structurally comparable and which fields still need semantic validation.