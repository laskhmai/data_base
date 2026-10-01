I need you to create a small READ-ONLY FOCUS POC using the existing database data.

IMPORTANT SAFETY RULE:
This task must be 100% READ ONLY.

Do NOT:
- INSERT
- UPDATE
- DELETE
- MERGE
- TRUNCATE
- CREATE
- ALTER
- DROP
- execute stored procedures that modify data
- modify any existing database object
- modify pipelines/configuration

You may only use SELECT queries to read data.

GOAL
====

Create an Excel workbook that shows what our existing Azure cost data would look like when represented using FOCUS-style columns.

This is a POC/analysis only.
Do NOT claim that a mapping is officially confirmed unless the data and semantics support it.

SOURCE TABLES
=============

1. dbo.ActualCost_RR
2. Cloudability.Daily_Spend

Cloudability must be filtered to:

vendor = 'Azure'

DATE RANGE
==========

First check:

SELECT MAX(Date)
FROM dbo.ActualCost_RR;

Then select the latest 2 COMPLETE dates that are also available in
Cloudability.Daily_Spend.

Do not use a partial/incomplete date.

If completeness cannot be confidently determined, show me the date counts
first and choose the latest two dates with stable/full-looking row counts.

RESOURCE MATCHING
=================

For comparison, use:

Date
+ Subscription
+ Normalized ResourceId

ActualCost_RR:
- SubscriptionId
- ResourceId

Cloudability:
- First validate that vendor_account_identifier represents the Azure
  SubscriptionId by comparing sample values with ActualCost_RR.SubscriptionId.
- Do not assume it only from the column name.

Cloudability resource_id can contain a prefix such as:

<GUID>::<ARM Resource ID>

For comparison, extract the ARM Resource ID after "::" when present.

Normalize ResourceId:
- trim spaces
- compare case-insensitively
- preserve the original values in the Excel output

IMPORTANT:
There can be multiple billing rows for the same resource on the same date.
Do not create a many-to-many join.

Where required, aggregate the appropriate cost/quantity fields at:

Date + SubscriptionId + Normalized ResourceId

before performing resource-level comparison.

FOCUS POC
=========

The MAIN Excel sheet should look like a FOCUS dataset.

The headers in the main sheet should be FOCUS field names,
NOT our internal source column names.

Use the available ActualCost_RR data to populate candidate FOCUS fields.

Start with fields such as:

BillingAccountId
BillingAccountName
SubAccountId
SubAccountName
ChargePeriodStart
BillingPeriodStart
BillingPeriodEnd
ResourceId
ResourceName
ServiceName
ServiceCategory
RegionId
RegionName
AvailabilityZone
ConsumedQuantity
ConsumedUnit
BilledCost
EffectiveCost
SkuId
SkuPriceId
ChargeCategory
PricingCategory
CommitmentDiscountId
CommitmentDiscountName
ProviderName
PublisherName
Tags

Use only fields for which we actually have source data.

Do NOT invent values just to populate a FOCUS column.

MAPPING
=======

Use the evidence already collected from our previous investigation.

Examples of candidate mappings include:

ActualCost_RR.Date
    -> ChargePeriodStart

ActualCost_RR.BillingPeriodStartDate
    -> BillingPeriodStart

ActualCost_RR.BillingPeriodEndDate
    -> BillingPeriodEnd

ActualCost_RR.BillingAccountId
    -> BillingAccountId

ActualCost_RR.BillingAccountName
    -> BillingAccountName

ActualCost_RR.SubscriptionId
    -> SubAccountId

ActualCost_RR.SubscriptionName
    -> SubAccountName

ActualCost_RR.ResourceId
    -> ResourceId

ActualCost_RR.ResourceName
    -> ResourceName

ActualCost_RR.Quantity
    -> ConsumedQuantity

ActualCost_RR.UnitOfMeasure
    -> ConsumedUnit

ActualCost_RR.ResourceLocation
    -> RegionId candidate

ActualCost_RR.MeterRegion
    -> RegionName candidate

ActualCost_RR.ServiceFamily
    -> ServiceCategory candidate

ActualCost_RR.ConsumedService / Product
    -> ServiceName candidate
    (validate which field is semantically appropriate)

ActualCost_RR.MeterId
    -> SkuId candidate

ActualCost_RR.PartNumber
    -> SkuPriceId candidate

ActualCost_RR.ReservationId
    -> CommitmentDiscountId candidate

ActualCost_RR.ReservationName
    -> CommitmentDiscountName candidate

ActualCost_RR.PublisherName
    -> PublisherName candidate

ActualCost_RR.Tags
    -> Tags

For COST fields be especially careful.

Do NOT automatically state:

Cost = EffectiveCost

or

Cloudability.amortized_spend = EffectiveCost

We already observed that individual matched rows can be very close,
but daily aggregate totals can differ significantly.

Keep cost mappings marked as CANDIDATE / NEEDS VALIDATION until semantics
are proven.

CLOUDABILITY VALIDATION
=======================

After generating the FOCUS-shaped data, compare corresponding information
against Cloudability.Daily_Spend.

Useful Cloudability fields include:

date
resource_id
vendor_account_name
vendor_account_identifier
vendor
amortized_spend
usage_quantity
service_name
item_description
region
region_zone
Azure_Resource_Name
part_number
reservation_identifier

For the selected two dates, calculate comparisons such as:

ResourceId match
Subscription match
ResourceName match
Service match
Quantity difference
Cost difference
Region comparison

For numeric fields calculate:

Actual value
Cloudability value
Absolute difference
Percentage difference

Do not label formatting differences as semantic mismatches.

EXCEL OUTPUT
============

Create:

FOCUS_POC.xlsx

Create these sheets:

1. FOCUS_Data

This is the MAIN deliverable.

It should contain FOCUS field names as headers and actual data underneath.

Do not make it look like an internal database dump.

2. Source_Mapping

Columns:

FOCUS Field
ActualCost_RR Source Column
Cloudability Comparison Column
Mapping Status
Notes

Mapping Status should use only:

DIRECT
CANDIDATE
DERIVED
NEEDS VALIDATION
NOT AVAILABLE

3. Cloudability_Comparison

Show side-by-side comparison for the same:

Date
Subscription
Normalized ResourceId

Include the relevant FOCUS value, ActualCost_RR source value,
Cloudability value, and comparison/difference.

4. Comparison_Summary

For each important field show:

FOCUS Field
Rows Compared
Matched Rows
Match %
Mismatch Rows
Mismatch %
Null/Unavailable Rows
Notes

Also include overall:

ActualCost_RR resource count
Cloudability Azure resource count
Matched resource count
ActualCost_RR-only count
Cloudability-only count

5. Daily_Cost_Comparison

Columns:

Date
ActualCost_RR Total Cost
Cloudability Total amortized_spend
Absolute Difference
Percentage Difference
Notes

IMPORTANT OUTPUT STYLE
======================

Keep the Excel professional and simple.

No unnecessary colors or AI-looking design.

Use:
- clear headers
- filters
- freeze top row
- sensible column widths
- numeric formatting for costs/quantities

The workbook should be understandable by someone who opens it without
reading the Python code.

PYTHON
======

Create a Python script to generate this Excel workbook.

Keep database queries READ ONLY.

Do not write anything back to SQL.

Save:
1. the Python script
2. FOCUS_POC.xlsx

Before executing anything, show me:

1. the SELECT queries you plan to run
2. the proposed FOCUS mapping
3. the Python approach

Wait for my approval before running the queries or generating the Excel.

After execution, give me a short summary of:
- which FOCUS fields were populated
- which mappings matched Cloudability strongly
- which fields differed
- cost differences
- any data gaps
- anything that still requires validation

Do not change any existing files/code/database objects unless I explicitly
approve it.