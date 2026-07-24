-- Fix staging table columns
ALTER TABLE [Silver].[Cloudability_Daily_Resource_Cost_GCP_Staging]
ALTER COLUMN overall_amortized_spend  DECIMAL(38,6)

ALTER TABLE [Silver].[Cloudability_Daily_Resource_Cost_GCP_Staging]
ALTER COLUMN overall_usage_quantity   DECIMAL(38,10)

-- Fix main table columns
ALTER TABLE [Silver].[Cloudability_Daily_Resource_Cost_GCP]
ALTER COLUMN overall_amortized_spend  DECIMAL(38,6)

ALTER TABLE [Silver].[Cloudability_Daily_Resource_Cost_GCP]
ALTER COLUMN overall_usage_quantity   DECIMAL(38,10)