I want to confirm which MongoDB FOCUS cost field we should use in our unified billing table. Please validate this using actual FOCUS and Direct Invoice CSV data, not assumptions.

**Step 1 — Select one organization and one completed invoice month**

Use the same organization and billing period from both API results. Read the existing Python scripts and available CSV files.

**Step 2 — Compare matching usage records**

Match FOCUS and Direct Invoice usage records using available identifiers such as:
- Organization ID and Invoice ID
- Project / SubAccountId
- ChargePeriodStart / usage date
- SKU / SkuMeter
- Region
- Usage quantity

If the records have different granularity, aggregate them to a common level before comparing. Identify any unmatched records.

**Step 3 — Validate the cost fields**

Compare:
1. FOCUS `PricingCurrencyEffectiveCost` against Direct Invoice usage `Amount`.
2. FOCUS `BilledCost` against Direct Invoice usage `Amount`.
3. FOCUS `EffectiveCost` against Direct Invoice usage `Amount`.
4. FOCUS `ListCost` against Direct Invoice usage `Amount`.

Show which field matches, the differences, and the currency and units used.

**Step 4 — Validate discounts and credits**

Inspect the actual Direct Invoice credit rows.

Determine whether those credits represent discounts already included in FOCUS `BilledCost` / `EffectiveCost`, or additional adjustments.

Do not assume every credit is a committed-spend discount. Identify credit types using actual source fields.

**Step 5 — Reconcile invoice totals**

Calculate and compare:
- Total FOCUS `PricingCurrencyEffectiveCost`
- Total FOCUS `BilledCost`
- Total FOCUS `EffectiveCost`
- Total Direct Invoice usage `Amount`
- Total Direct Invoice credits
- Direct Invoice net billed amount

Verify whether:

`FOCUS PricingCurrencyEffectiveCost + Invoice Credits = Invoice Net Billed Amount`

Also check whether:

`FOCUS BilledCost = Invoice Net Billed Amount`

Explain any differences and avoid double-counting discounts.

**Step 6 — Final recommendation**

Tell me:
- Which field should represent usage charges in our unified table?
- Which field should represent post-discount daily cost?
- Which invoice credit rows should be added separately?
- How can we prevent double-counting?
- What is the exact mapping for our unified MongoDB staging table?

Show actual evidence with a few matching usage records and invoice-level reconciliation totals.

Clearly distinguish confirmed findings from assumptions or unresolved gaps.

**IMPORTANT: This is read-only analysis. Do not modify Python scripts, SQL tables, or database records. Do not print API keys, passwords, or connection strings.**