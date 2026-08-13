
=======
GCP Cost Anomaly Validation

Objective:
Validate the GCP cost anomaly detection process to confirm that the model is correctly identifying unusual resource-level cost increases and determine whether any real anomalies are being missed.

Work involved:

Run the provided GCP anomaly Python script for multiple dates, preferably around 5 days.
Backfill missing dates if the scheduled process hasn't populated them.
Validate records inserted into the Gold Anomaly Results table.
Compare Gold anomaly records against the Aggregation table's daily resource costs.
Confirm whether anomalies with overspend/delta > $50 are genuine.
Perform the reverse validation:
Aggregation → Gold
Find resources with sudden daily cost increases.
Use approximately $50 delta initially (or $25 during deeper testing).
Check whether those resources were detected by the anomaly model.
Identify false positives — Gold says anomaly, but cost behavior doesn't support it.
Identify false negatives / missed anomalies — aggregation shows unusual increase, but Gold doesn't contain the resource.
For missed anomalies, check whether the resource has at least 30 days of historical cost data. Newly created resources with only ~14 days of history may not be eligible for reliable anomaly detection.
Document findings across the tested dates, including why anomalies were detected or missed.
Email functionality in the script can remain commented out during validation.
Expected outcome

At the end of the story, you should be able to report something like:
