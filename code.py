Using read-only SELECT queries only, validate data availability in dbo.ActualCost_RR for the FOCUS-related columns from the structural matrix.
Use the latest 30 days actually available in the table, not today's last 30 days.
For every candidate FOCUS field, return:
FOCUS field | ActualCost_RR source column | total rows | non-null/non-empty rows | populated % | 3 redacted sample values | current classification
Pay special attention to:
BillingAccountId, BillingAccountName, SubscriptionId, SubscriptionName, Date, BillingPeriodStartDate, BillingPeriodEndDate, ResourceId, ResourceName, ConsumedService, Product, MeterName, ServiceFamily, MeterCategory, MeterSubCategory, ResourceLocation, MeterRegion, AvailabilityZone, Quantity, UnitOfMeasure, Cost, EffectivePrice, UnitPrice, MeterId, PartNumber, ChargeType, PricingModel, ReservationId, ReservationName, PublisherName, Tags.
Do not create or modify anything. No INSERT, UPDATE, DELETE, MERGE, TRUNCATE, CREATE, DROP, ALTER, EXEC, stored procedures, files, or pipelines.
Do not change semantic mappings yet. This step is only to determine whether each Azure candidate field actually has usable data.
At the bottom give three groups:
POPULATED, PARTIALLY POPULATED, EMPTY/NOT AVAILABLE.