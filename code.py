Use all previous context from this conversation.

I want to correct the investigation approach before we make any POC decision.

The previous investigation is useful, but DO NOT assume that Silver,
Gold, Cloudability, Azure, or GCP tables are automatically the final
or authoritative source just because a table already looks consolidated.

Some Silver and Gold tables in our environment were themselves created
by joining and transforming multiple source tables/databases.

For example, a Silver or Gold table may combine Azure data with other
internal sources such as SAP HANA or other databases.

For this POC, I do NOT currently care how a Silver/Gold table was built.

My main question is:

WHAT DATA DO WE ACTUALLY HAVE ACROSS THE DATABASE THAT CAN REPRESENT
THE FOCUS CONCEPTS FOR AZURE AND GCP?

This must be a comprehensive DATA-LEVEL discovery, not only a
column-name/schema-name search.


STRICT READ-ONLY MODE

You may only inspect metadata, object definitions and data using
read-only operations.

DO NOT:
- INSERT
- UPDATE
- DELETE
- MERGE
- TRUNCATE
- CREATE
- ALTER
- DROP
- execute data-changing stored procedures
- modify code
- modify files
- modify pipelines
- modify configuration
- create temporary/permanent database objects

If any operation is not clearly read-only, stop and ask me first.


========================================================
PHASE 1 — INVENTORY THE RELEVANT DATABASE
========================================================

Scan the accessible database broadly.

Identify ALL accessible tables and views that could contain information
relevant to cloud inventory, cost, billing, usage, pricing, resources,
subscriptions, accounts, projects, services, SKUs, reservations,
commitments, regions, zones, tags, labels or cloud metadata.

Do not restrict the search to known tables such as:

AZURE.Resources
GCP.Assets
Cloudability.Daily_Spend
Silver.Cloudability_Daily_Resource_Cost

Those are only known examples.

Search all relevant schemas/tables/views.

Include sources related to:

Azure
GCP
Cloudability
Silver
Gold
CostAtlas
inventory
billing
cost
usage
pricing
retail pricing
subscriptions
projects
accounts
resources
meters
SKUs
reservations
commitments
tags
labels
properties
categories
quota

and any other relevant objects discovered during the scan.


========================================================
PHASE 2 — READ THE DATA, NOT ONLY THE COLUMN NAMES
========================================================

For every relevant accessible table/view discovered:

1. Capture schema.table/view name.

2. Capture its columns and data types.

3. Read a small, safe, representative sample of actual rows.

4. For every column, inspect enough representative non-null values to
   understand what type of information the column actually contains.

Do NOT decide column meaning only from its name.

For example, a column may:
- contain multiple values concatenated together
- contain JSON
- contain key/value data
- contain tags
- contain properties
- contain nested resource metadata
- contain IDs with useful components embedded inside them
- contain service/SKU/pricing information under a different name
- contain calculated/transformed values

Where useful, inspect distinct/sample non-null values rather than only
the first rows, because the first rows may contain NULL or atypical data.

Keep queries small and safe. Do not perform expensive unrestricted scans.


========================================================
PHASE 3 — DEEPLY INSPECT STRUCTURED/TEXT COLUMNS
========================================================

Pay special attention to columns containing:

JSON
Tags
Properties
Labels
Resource_JSON
usage_types
metadata
details
attributes
dimensions
identifiers
pricing metadata
reservation metadata
service/SKU metadata

Inspect their actual structure and available keys.

If one column contains several business values, document them separately.

Example:

A value such as:

"East US 2::eastus2"

may contain two concepts:

RegionName = East US 2
RegionId   = eastus2

Similarly, if JSON contains:

service
sku
zone
project
resource
pricing
unit
reservation

document those values even if there is no dedicated SQL column for them.


========================================================
PHASE 4 — UNDERSTAND TRANSFORMED VALUES WHERE NECESSARY
========================================================

If a useful value appears transformed or concatenated, inspect existing
view/stored-procedure/code definitions READ-ONLY only when necessary to
understand that value.

Look for existing logic such as:

JSON_VALUE
JSON_QUERY
CONCAT
SUBSTRING
STRING_SPLIT
CASE
CAST
REPLACE
COALESCE
joins
aliases
calculations

But do NOT spend time tracing complete pipeline/orchestration lineage.

I do not need to know every job or staging process that created a table.

I only need enough transformation evidence to correctly understand the
data currently stored in it.


========================================================
PHASE 5 — USE FOCUS V1.2 AS THE TARGET MODEL
========================================================

After understanding the available data, compare it semantically against
FOCUS v1.2 concepts.

Investigate at least:

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
CommitmentDiscountCategory

Tags


========================================================
PHASE 6 — COST / PRICE SEMANTICS ARE VERY IMPORTANT
========================================================

Do not treat all monetary values as "cost".

For every relevant monetary field determine, as far as the evidence
allows, whether it represents:

unit price
retail/list unit price
extended list cost
actual/billed cost
amortized/effective cost
contracted/negotiated cost
reservation price
commitment cost
fee
discount
credit

For example:

retailPrice = 4.83

does NOT automatically mean:

ListCost = 4.83

It may be a unit price that requires quantity and unit normalization.

Likewise:

amortized_spend

must not automatically be declared FOCUS EffectiveCost merely because
the names sound similar.

Use actual values, surrounding fields and existing calculation logic
where available.


========================================================
PHASE 7 — DO THE SAME FOR AZURE AND GCP
========================================================

The objective is not just Azure.

For each useful FOCUS concept determine:

Where does Azure information exist?

Where does GCP information exist?

The Azure and GCP source tables/column names do NOT need to be the same.

For example conceptually:

Azure source A.column_x ─┐
                         ├──> FOCUS ResourceId
GCP source B.column_y ───┘

That is exactly the standardization we want to demonstrate.


========================================================
PHASE 8 — CLASSIFY BASED ON DATA EVIDENCE
========================================================

For every FOCUS concept classify the result as:

DIRECT
A dedicated existing value represents the concept.

EXTRACTABLE
The information exists inside JSON/tags/properties/text/identifier
and can be reliably extracted.

DERIVABLE
The value can be reliably calculated from existing data.

POSSIBLE / NEEDS VALIDATION
Related data exists but semantic equivalence is not yet proven.

NOT FOUND AFTER DATA-LEVEL SEARCH
Only use this classification after relevant tables, actual values,
nested structures and existing transformations have been inspected.

Do NOT say "NOT FOUND" simply because an exact FOCUS column name
does not exist.


========================================================
PHASE 9 — BUILD A DATA AVAILABILITY MATRIX
========================================================

Return:

FOCUS Field
| Azure Source schema.table.column
| GCP Source schema.table.column
| Actual Sample Value/Structure
| DIRECT / EXTRACTABLE / DERIVABLE / POSSIBLE / NOT FOUND
| Transformation Needed
| Confidence
| Evidence/Reasoning

If multiple tables contain the same information, list the candidates.

Do not automatically choose Silver or Gold.

Recommend the simplest reliable source based on the actual data.


========================================================
PHASE 10 — THEN DESIGN THE MINIMUM POC
========================================================

ONLY after completing the data investigation, tell me what a practical
first FOCUS-shaped POC can contain.

The goal is:

Different Azure internal fields ─┐
                                 │
                                 ├── Common FOCUS-shaped structure
                                 │
Different GCP internal fields ───┘

We want the smallest meaningful set of fields that demonstrates
multi-cloud standardization.

Do not try to fill every FOCUS field.

Do not force mappings.

Do not build the POC yet.

Do not create a table.

Do not write ETL.

Do not modify FOCUS_MAPPING.md yet.


========================================================
FINAL QUESTIONS TO ANSWER
========================================================

After the investigation answer:

1. What FOCUS-relevant information do we already have for Azure?

2. What FOCUS-relevant information do we already have for GCP?

3. Which information exists directly?

4. Which information is hidden inside JSON/tags/properties/concatenated
   fields and can be extracted?

5. Which information can be derived from multiple existing fields?

6. Which mappings are still only assumptions?

7. Which FOCUS concepts are genuinely not present after DATA-LEVEL
   investigation?

8. What is the smallest Azure + GCP FOCUS-shaped dataset we can build
   entirely from existing internal data?

9. Which fields, if any, would actually require external Azure/GCP
   billing or FOCUS export data?

IMPORTANT:
Take the time needed to inspect the relevant data properly.
Accuracy is more important than quickly producing a mapping.