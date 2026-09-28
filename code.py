Use all of the previous Cloudability investigation and evidence already available in this conversation as the starting context.

Do not restart the investigation from scratch.

Based on what we already traced about the Cloudability API, Python collectors, ADLS, external tables, staging tables, stored procedures, Cloudability.Spend / Properties / Categories / Tags, and Daily_Spend_Aggregate, continue from our current point.

Our next objective is to understand the exact meaning and lineage of each Cloudability.Daily_Spend column so that we can build an evidence-based candidate mapping to FOCUS v1.2.

Reuse the SQL/code evidence we already collected. Only run additional READ-ONLY queries or inspect code where information is missing or needs confirmation.

Do not modify any database, table, stored procedure, code, file, pipeline, or configuration.

Do not assume that similarly named Cloudability and FOCUS columns are equivalent.

For each Daily_Spend column, establish:
- what it actually represents
- where it originates
- any transformation applied
- whether it is raw, derived, tag-based, or company-specific
- its best potential FOCUS v1.2 mapping
- whether that mapping can be confirmed from definitions or still requires actual Azure FOCUS sample data

Then use the detailed FOCUS mapping instructions I provided below this message.

The goal at this stage is NOT to build the FOCUS pipeline.
The goal is to produce a reliable candidate mapping and identify exactly what sample Azure FOCUS data we need for validation.