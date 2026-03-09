Ingest Azure Reservations Data via Reservations API

Description

Implement a pipeline to retrieve Azure reservation data using the Azure Reservations API and populate the Cloudability.Azure_Reservations table.

The pipeline will call the appropriate Reservations API endpoint (billing account or billing profile), extract required reservation attributes, and insert or update records in the target table using reservation_id as the unique key.

Key fields include reservation identifier, resource type, SKU description, purchase date, expiration date, billing scope, quantity, and utilization metrics (1-day, 7-day, 30-day).

Acceptance Criteria

Reservations API endpoint is successfully called.

API response is parsed and mapped to Cloudability.Azure_Reservations.

Reservation records are inserted or updated using reservation_id.

Required fields are populated:

reservation_id

reservation_type

reserved_resource_type

sku_description

purchase_date

expiration_date

billing_scope_id

quantity

utilization metrics

Pipeline execution logs errors for failed API calls or data loads.