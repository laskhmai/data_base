I am working on a FOCUS POC and need to map our existing
[Cloudability].[Daily_Spend] columns to the FOCUS schema.

Before doing any FOCUS mapping, I want you to fully understand what each
Daily_Spend column actually represents from our existing implementation.

IMPORTANT:
- Do not make any changes to code, database objects, tables, stored procedures,
  pipelines, or data.
- Do not execute any stored procedure or write query.
- Use READ-ONLY investigation only.
- Do not assume column meanings from names alone.
- Trace the actual SQL/code and show evidence for your conclusions.

Please do the following:

1. Get the complete column list for:
   [Cloudability].[Daily_Spend]

2. Trace how [Cloudability].[Daily_Spend] is populated.
   Start from the procedure that inserts into it, including:
   [Cloudability].[Daily_Spend_Aggregate]
   and trace backward through all source tables/views/procedures involved.

3. For EACH Daily_Spend column, identify:
   - Daily_Spend column name
   - Exact source table
   - Exact source column
   - Any transformation / CASE / CAST / calculation applied
   - What the field represents based on the implementation
   - Whether it is a raw Cloudability field, tag, derived field,
     internal/company-specific enrichment, or ETL metadata

4. Pay special attention to fields such as:
   - date
   - invoice_date
   - resource_id
   - vendor_account_name
   - resource_key
   - vendor
   - amortized_spend
   - DBCU_converted_usage
   - cleardata_fee
   - usage_quantity
   - instance_type
   - instance_category
   - instance_family
   - instance_size
   - operating_system
   - operation
   - item_description
   - service_name
   - enhanced_service_name
   - region
   - zone
   - region_zone
   - vendor_account_identifier
   - reservation_identifier
   - usage_family
   - usage_type
   - Humana_Application_ID
   - Humana_Resource_ID(tag23)
   - Azure_Resource_Name
   - Azure_Resource_Group(tag11)
   - Environment(tag3)
   - AWS_server_name(tag5)
   - AWS_server_description(tag18)
   - tag27
   - part_number
   - CloudTenancy
   - Container_Cluster_Name
   - Container_Namespace
   - groupname4
   - updated_date

5. Also inspect upstream collector/staging logic where necessary so we know the
   ORIGINAL source/meaning of the field, not just that it came from
   Cloudability.Spend, Properties, Tags, or Categories.

6. Specifically verify the existing suspected tag18 logic:
   Check whether AWS_server_description(tag18) is actually populated from
   tag18 or whether the current SQL uses tag5.
   Do not call it a bug; just report exactly what the code currently does.

7. After tracing the current implementation, create a mapping table with:

   Daily_Spend Column
   | Actual Source
   | Source Column
   | Transformation
   | What It Represents
   | Field Type (Cost / Usage / Resource / Account / Service / SKU /
     Location / Tag / Internal / ETL)
   | Potential FOCUS Mapping
   | Mapping Confidence (High / Medium / Low)
   | Reason / Evidence

8. For the "Potential FOCUS Mapping" column:
   Use FOCUS v1.2 concepts/column names where you are confident.
   Do NOT force a mapping.
   If a Daily_Spend field is company-specific, derived, or has no obvious
   FOCUS equivalent, clearly mark it as:
   "No direct FOCUS mapping / enrichment required"
   or
   "Needs validation".

9. I especially want to understand whether these mappings are actually valid:
   resource_id -> ResourceId
   service_name -> ServiceName
   region -> RegionName
   Azure_Resource_Name -> ResourceName
   usage_quantity -> ConsumedQuantity
   vendor -> ProviderName
   amortized_spend -> EffectiveCost

   Validate them from our current field meaning rather than accepting them
   just because the names look similar.

10. At the end give me three sections:

   A. HIGH-CONFIDENCE FOCUS MAPPINGS
      Fields we can reasonably map now.

   B. NEEDS ACTUAL FOCUS SAMPLE DATA
      Fields where definitions look similar but values must be compared.

   C. NO DIRECT FOCUS MAPPING / COMPANY-SPECIFIC
      Tags, derived fields, internal enrichment, ETL fields, etc.

Also tell me what minimum columns and sample date range we should request
from an Azure FOCUS export so that we can validate this POC against
Cloudability.Daily_Spend.

For every important conclusion, show the SQL/code/object that supports it.
If something cannot be proven from the repository/database, mark it
"NOT CONFIRMED" instead of guessing.