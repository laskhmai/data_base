Work in strict READ-ONLY mode only. Do not create, insert, update, delete, merge, truncate, alter, drop, execute stored procedures, or modify any files other than producing a local analysis output if needed.
My immediate goal is NOT to investigate the total cost difference yet.
First create a concise list of the approximately 15 FOCUS v1.2 columns we actually need for the current POC.
For each column show:
1. FOCUS column name
2. Current dbo.ActualCost_RR source column
3. Closest Cloudability.Daily_Spend column
4. Sample value
5. Status: DIRECT / DERIVED / CANDIDATE / NEEDS VALIDATION / NOT AVAILABLE
6. Short reason
Then specifically validate the concern around ChargeCategory.
If ChargeType / proposed ChargeCategory is mostly Usage, determine whether the detailed usage type can still be understood from related fields such as:
- ConsumedService
- Product
- MeterName
- ServiceFamily
- MeterCategory
- MeterSubCategory
- PricingModel
- ReservationId
- ReservationName
- PartNumber
- UnitOfMeasure
Show distinct values and population percentages for these fields for a recent complete sample period.
Also show:
- percentage of rows where ChargeType is Usage
- all other ChargeType values and their percentages
- ReservationId and ReservationName populated percentage
- PricingModel populated percentage
- examples showing whether storage, VM, database, networking, reservation, or other charge types can be distinguished even when ChargeType says Usage
Do not assume missing reservation fields are caused by permissions. Just report what the data proves.
Do not make any final claim about Cost, BilledCost, EffectiveCost, discounts, or Cloudability amortized_spend yet.