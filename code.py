Review our existing MongoDB FOCUS API and Direct Invoice API comparison and the proposed single-table design.

Our requirement is to maintain one MongoDB cost dataset that includes daily usage charges, discounts, credits, and invoice reconciliation information.

Before modifying any code or SQL tables:

1. Identify which FOCUS cost field exactly matches the Direct Invoice usage amount, including currency and units.
2. Determine whether FOCUS BilledCost/EffectiveCost already includes the discounts represented by Direct Invoice credit rows.
3. Separate SKU-level discount credits from invoice-level credits and other adjustments.
4. Identify the correct cost field and sign for each row type in a unified dataset.
5. Confirm how to avoid double-counting usage charges or discounts.
6. Identify the appropriate organization, project, invoice, date, and SKU identifiers for each row type.
7. Propose a unified FOCUS-based schema that preserves all required billing and resource details.
8. Demonstrate reconciliation from the unified dataset to the actual invoice totals using our existing CSV data.

Do not modify the database or existing Python scripts yet. Provide the proposed schema, mapping rules, reconciliation evidence, and any unresolved issues first.