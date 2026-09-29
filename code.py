Use all previous context and findings from this conversation.

I want to simplify and refocus the investigation.

We already spent enough time understanding the Cloudability pipeline.
Do NOT continue tracing staging tables, orchestration, pipeline schedules,
or stored-procedure execution unless it is absolutely necessary to identify
a data field needed for the POC.

This is now a DATA DISCOVERY exercise for a FOCUS POC.

STRICT READ-ONLY MODE:
Do not modify any data, database object, stored procedure, file, code,
pipeline, or configuration.
Do not execute write operations or data-changing stored procedures.
Only inspect metadata, definitions, and data using safe read-only queries.

OBJECTIVE

FOCUS v1.2 is our target/common data model.

I want to determine how much of the data needed to demonstrate a
FOCUS-standardized dataset already exists in our databases.

We have many schemas, tables and views. Do not restrict the investigation
to Cloudability.

Search broadly across the available database for relevant Azure, GCP,
Cloudability, inventory, billing, cost, usage, pricing, subscription,
project/account, resource, tag/label and commitment data.

Known examples include Azure.Resources and GCP.Assets, but do not limit
the search to those tables.


STEP 1 — DEFINE THE TARGET

Use the FOCUS v1.2 columns/concepts as the target.

Prioritize the fields that are useful for a practical POC, especially:

ProviderName

BillingAccountId
BillingAccountName
SubAccountId
SubAccountName

ResourceId
ResourceName
ResourceType

ServiceName
ServiceCategory
ServiceSubcategory

RegionId
RegionName
AvailabilityZone

ChargePeriodStart
ChargePeriodEnd
BillingPeriodStart
BillingPeriodEnd

BilledCost
EffectiveCost
ListCost
ContractedCost

ConsumedQuantity
ConsumedUnit

PricingQuantity
PricingUnit

SkuId
SkuPriceId

ChargeCategory
ChargeDescription

CommitmentDiscountId
CommitmentDiscountType

Tags

Do not assume that every FOCUS column is required for the first POC.
The purpose is to find the smallest useful standardized dataset that
clearly demonstrates Azure and GCP in the same structure.


STEP 2 — SEARCH OUR EXISTING DATA

Search database metadata broadly to identify candidate tables/columns
for these concepts.

Look across:

- Azure inventory/resource tables
- Azure subscription/account tables
- Azure cost/billing tables
- Azure usage tables
- Azure retail/list pricing tables

- GCP asset/inventory tables
- GCP project/account tables
- GCP billing/cost tables
- GCP usage/quota tables
- GCP pricing tables

- Cloudability tables

- any other relevant schemas/tables/views

Use table names, column names, object definitions and, where useful,
small safe samples of actual values.

Do not search every object deeply just because it exists.
Use FOCUS concepts to drive the search.


STEP 3 — CREATE A DATA AVAILABILITY MAP

For each useful FOCUS concept, return:

FOCUS Field
| Azure Candidate Table.Column
| GCP Candidate Table.Column
| Cloudability Candidate Table.Column
| Example Value
| Meaning
| Confidence
| Notes / Transformation Needed

If multiple internal sources could provide the same field, list them
and recommend the simplest/most authoritative candidate for the POC.

Do not force mappings.

If a field cannot be found internally, mark:

NOT FOUND INTERNALLY


STEP 4 — COST AND PRICING

Pay particular attention to cost and pricing.

Determine whether we already have internal data representing concepts like:

- actual/billed cost
- amortized/effective cost
- list/retail cost
- contracted/discounted cost
- usage quantity
- usage unit
- pricing quantity
- pricing unit

For Azure and GCP, identify the actual tables/columns where these values
exist, if available.

Do not assume Cloudability.amortized_spend is identical to FOCUS
EffectiveCost. Treat it as a candidate until semantics/values support it.


STEP 5 — RESOURCE MATCHING

Determine whether Azure.Resources, GCP.Assets and Cloudability.Daily_Spend
contain identifiers that allow us to relate the same resources.

For example:

Azure resource ID
GCP resource identifier
Cloudability resource_id

Show a few safe examples of identifier formats and explain whether joins
are realistically possible.

Do not perform large joins yet.


STEP 6 — DESIGN THE MINIMUM POC

Based only on data that actually exists internally, propose the smallest
useful FOCUS-shaped POC dataset.

The goal is to demonstrate:

Azure data ─┐
            ├──> SAME standardized columns
GCP data ───┘

For example, if supported by our data:

ProviderName
ResourceId
ResourceName
ResourceType
ServiceName
RegionName
EffectiveCost
ConsumedQuantity
ConsumedUnit
Tags

But choose the final POC fields based on evidence from our databases,
not this example.


IMPORTANT SCOPE CONTROL

Do NOT build anything yet.

Do NOT create a FOCUS table.

Do NOT write ETL.

Do NOT modify FOCUS_MAPPING.md yet.

Do NOT investigate unrelated pipeline architecture.

First give me the discovery results.


FINAL OUTPUT

Give me:

1. What useful Azure data already exists internally.
2. What useful GCP data already exists internally.
3. What useful cost/pricing/usage data already exists internally.
4. A FOCUS-to-existing-data availability matrix.
5. Which fields can support a simple Azure + GCP standardized POC now.
6. Which important FOCUS fields are genuinely missing.
7. Whether we can build the first POC entirely from existing DB data,
   or whether an external Azure/GCP FOCUS export is actually necessary.

For every conclusion, provide the actual schema.table.column evidence.

Keep the investigation practical.
The goal is a small, understandable POC, not a full production architecture.