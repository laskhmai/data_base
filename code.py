We are working on MongoDB billing data integration for our Cloud 3.0 project. Currently, we are exploring two APIs: FOCUS API and Direct Invoice API, to understand what information each provides and which one is suitable for our cost pipeline.
What we are doing with the FOCUS API
The FOCUS API provides standardized MongoDB cost and usage data. It includes organization and project information, service names, SKU details, regions, usage dates, usage quantities, unit pricing information, list cost, billed cost, and effective cost.

We are using the FOCUS API to understand daily usage and how much each service or resource costs. We are also mapping the available fields into our staging table so that MongoDB billing data can follow the same structure as other cloud cost data.

However, the FOCUS report does not clearly provide separate invoice-level credit transactions or the final outstanding invoice balance. Also, some resource-level fields may not be available in the report.
What we are doing with the Direct Invoice API
The Direct Invoice API provides MongoDB's billing and invoice information. It includes invoice details, usage line items, service and SKU information, regions, quantities, unit pricing, charges, and credit or adjustment information.

We are using this API to understand how MongoDB calculates invoice charges, what credits are applied, and how those adjustments affect the invoice balance.

Unlike FOCUS, the Direct Invoice API is more focused on billing reconciliation rather than providing a standardized cloud cost dataset.
What we are trying to identify
Our main objective is to compare both APIs and understand:
- Whether both APIs contain the same underlying usage records.
- Whether service names, SKUs, regions, quantities, and unit prices match.
- How FOCUS calculates billed and effective costs.
- How the Direct Invoice API represents discounts and credits.
- Whether any fields or usage records are missing from either API.
- Which API is more suitable for daily cost ingestion and which is needed for invoice reconciliation.
Our next step
We are fetching data from both APIs for the same organizations and billing periods, then comparing the records at the service, SKU, region, and usage-date level.