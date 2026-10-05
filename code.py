I need you to perform a READ-ONLY validation of our existing Azure cost table against Microsoft's official FOCUS conversion mapping.

IMPORTANT SAFETY RULES:
- READ ONLY.
- Do NOT INSERT, UPDATE, DELETE, MERGE, TRUNCATE, DROP, ALTER, CREATE, or execute stored procedures.
- Do NOT modify any database objects, code, files, pipelines, configurations, or data.
- Only use SELECT queries, metadata/schema inspection, and existing documentation/code.
- If anything requires a write/change, stop and ask me first.

Context:
We are evaluating how our existing Azure cost data can support the FinOps FOCUS schema.

Our existing table is:

dbo.ActualCost_RR

Microsoft documentation shared by our manager:
"Convert Cost Management data to FOCUS"

Use Microsoft's official FOCUS conversion mapping as the source of truth.

Do NOT use Cloudability columns to define the FOCUS schema.
Cloudability can be used later only for validation/comparison.

TASK 1 — Inspect ActualCost_RR

First inspect the complete schema of:

dbo.ActualCost_RR

Return:
- Column name
- Data type
- Nullable/not nullable

Do not assume a column is missing until you inspect the complete table schema.

TASK 2 — Compare with Microsoft FOCUS requirements

Compare the Microsoft Cost Management → FOCUS source requirements against the columns actually available in dbo.ActualCost_RR.

At minimum investigate these FOCUS fields:

- BilledCost
- BillingAccountId
- BillingAccountName
- BillingAccountType
- BillingCurrency
- BillingPeriodStart
- BillingPeriodEnd
- ChargeCategory
- ChargeClass
- ChargeDescription
- ChargeFrequency
- ChargePeriodStart
- ChargePeriodEnd
- CommitmentDiscountCategory
- CommitmentDiscountId
- CommitmentDiscountName
- CommitmentDiscountStatus
- CommitmentDiscountType
- ConsumedQuantity
- ConsumedUnit
- ContractedCost
- ContractedUnitPrice
- EffectiveCost
- InvoiceIssuerName
- ListCost
- ListUnitPrice
- PricingCategory
- PricingCurrency
- PricingQuantity
- PricingUnit
- ProviderName
- PublisherName
- RegionId
- RegionName
- ResourceId
- ResourceName
- ResourceType
- ServiceCategory
- ServiceName
- ServiceSubcategory
- SkuId
- SkuMeter
- SkuPriceId
- SubAccountId
- SubAccountName
- Tags

Also include any additional FOCUS 1.2 fields present in the Microsoft mapping that I have not listed above.

TASK 3 — Produce one comparison table

Create a result in this exact style:

| FOCUS Column | Microsoft Cost Management Source Field(s) | ActualCost_RR Column | Status | Transformation / Rule | Evidence / Notes |

For Status, use ONLY:

DIRECT
AVAILABLE - TRANSFORMATION NEEDED
AVAILABLE - LOOKUP NEEDED
DIFFERENT FIELD - NEEDS VALIDATION
MISSING
NOT CONFIRMED

Examples:

ChargePeriodStart | Date | Date | DIRECT | None | Exact source column exists

SubAccountId | SubscriptionId | SubscriptionId | DIRECT | None | Exact source column exists

ChargeCategory | ChargeType | ChargeType | AVAILABLE - TRANSFORMATION NEEDED | Apply Microsoft's ChargeType → FOCUS ChargeCategory rules | Source exists

ConsumedQuantity | Quantity | Quantity | DIRECT/TRANSFORMATION depending on Microsoft rule | Explain rule | Source exists

CommitmentDiscountId | BenefitId | ? | NOT CONFIRMED or MISSING | Explain | Do not automatically substitute ReservationId

CommitmentDiscountName | BenefitName | ? | NOT CONFIRMED or MISSING | Explain | Do not automatically substitute ReservationName

EffectiveCost | CostInBillingCurrency + Microsoft Actual/Amortized logic | Cost or other available field | DIFFERENT FIELD - NEEDS VALIDATION | Explain Microsoft rule | Do NOT assume Cost = EffectiveCost

BilledCost | CostInBillingCurrency + Microsoft rules | Cost or other available field | DIFFERENT FIELD - NEEDS VALIDATION | Explain | Do NOT assume equivalence

TASK 4 — Specifically investigate these source fields

Check whether ActualCost_RR contains these fields directly, under another name, or not at all:

- BenefitId
- BenefitName
- CostInBillingCurrency
- Frequency
- AdditionalInfo
- PricingModel
- ChargeType
- Quantity
- UnitOfMeasure
- MeterName
- ResourceId
- ResourceName
- ResourceType
- ConsumedService
- SubscriptionId
- SubscriptionName
- Tags

If a similar field exists, do NOT automatically map it.

For example:

ReservationId != automatically BenefitId
ReservationName != automatically BenefitName
Cost != automatically CostInBillingCurrency / BilledCost / EffectiveCost

Mark those as NEEDS VALIDATION unless evidence proves equivalence.

TASK 5 — Check Actual vs Amortized requirement

Microsoft's FOCUS conversion documentation says conversion requires both Actual Cost and Amortized Cost datasets.

Determine from available evidence whether dbo.ActualCost_RR represents:

- Actual Cost only
- Amortized Cost only
- Combined Actual + Amortized
- Cannot be determined

Show the evidence for the conclusion.

Do NOT guess from the table name alone.

Check available columns, values, code/lineage, views, stored procedures, external tables, or existing documentation using READ-ONLY inspection.

If the upstream source cannot be accessed because of permissions, state:

"NOT CONFIRMED - upstream source inaccessible"

Do not assume it is a permission issue unless there is actual evidence of access denial.

TASK 6 — Validate population

For every ActualCost_RR field used in the proposed mapping, use SELECT-only queries to calculate:

- Total rows
- Non-null rows
- Non-null %
- Blank/empty rows where applicable
- Distinct count
- 3 safe sample values

Use the latest stable date range available in the table.

Clearly state the date range used.

Do not expose sensitive values or PII.

TASK 7 — Final summary

After the detailed table, provide these counts:

1. DIRECT
2. TRANSFORMATION NEEDED
3. LOOKUP NEEDED
4. NEEDS VALIDATION
5. MISSING
6. NOT CONFIRMED

Then provide four short sections:

A. What we already have in ActualCost_RR
B. What can be derived using Microsoft's documented transformation
C. What data is genuinely missing
D. What requires another API/export/dataset or additional access

Most importantly, answer:

"Can dbo.ActualCost_RR alone produce a complete Microsoft FOCUS dataset?"

Answer YES, NO, or NOT YET CONFIRMED and provide evidence.

Also answer:

"If not, exactly which source fields/datasets are still required?"

Do not compare to Cloudability in this step unless needed only as secondary validation.

Do not modify anything.

At the end, save the findings into a simple Markdown file named:

FOCUS_ACTUALCOST_RR_MAPPING.md

The document should be plain and professional, with no unnecessary styling.