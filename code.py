-- Fix staging table
ALTER TABLE [Silver].[Cloudability_Daily_Resource_Cost_GCP_Staging]
ALTER COLUMN overall_amortized_spend FLOAT

ALTER TABLE [Silver].[Cloudability_Daily_Resource_Cost_GCP_Staging]
ALTER COLUMN overall_usage_quantity FLOAT

-- Fix main table
ALTER TABLE [Silver].[Cloudability_Daily_Resource_Cost_GCP]
ALTER COLUMN overall_amortized_spend FLOAT

ALTER TABLE [Silver].[Cloudability_Daily_Resource_Cost_GCP]
ALTER COLUMN overall_usage_quantity FLOAT