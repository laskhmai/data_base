Analyze the existing local September 2026 MongoDB Native FOCUS API and Direct Invoice API CSV files. READ ONLY. No API calls, no database access, no file creation or modification. Print all findings in terminal/chat.

Goal: Answer whether invoice credits reduce actual payable cost and how to represent this alongside FOCUS BilledCost.

1. Pick one real SKU that has matching usage records in both datasets. Use the exact SKU and project identifiers from the files; do not invent values.
2. Show FOCUS fields: ChargePeriodStart, SubAccountId, SkuMeter, ListCost, ContractedCost, BilledCost, EffectiveCost, ContractApplied, x_PricingCurrencyRate.
3. Show matching Direct Invoice usage rows: Usage Date, Project ID, SKU, Amount, Discount Percent, Description, Note, and any currency/unit information.
4. Reconcile usage totals for the SAME project, SKU, and date range, including observed 0.7 conversion, without assuming that Amount is USD.
5. Find related negative Credit rows. Check whether those rows actually contain SKU or resource identifiers. If not, do not allocate project-level credits to the selected SKU as though the allocation were verified.
6. For the selected project, show gross usage, credit rows, invoice net, FOCUS BilledCost, and explain what each figure represents. Show whether a remaining credit balance, prepaid commitment drawdown, or final payable amount can be established from these CSVs.
7. State which additional billing/contract fields or API response would be needed to prove the real payable amount and remaining credit balance.
8. Finish with a short teammate-friendly conclusion, marking VERIFIED vs UNKNOWN.

No files, no modifications, no secrets in output.