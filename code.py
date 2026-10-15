Hi bro, I did some analysis on MongoDB September billing data by comparing the **FOCUS API and Direct Invoice API**. I downloaded both CSV files through Postman and compared the columns, usage costs and credits.

On the FOCUS API side, we are getting 49 columns, and around 10 columns are directly matching with our 14 FOCUS staging table columns. For September, we got around 4,081 usage records in both APIs, and they matched.

Coming to cost, in the full export, Direct Invoice API shows 6,340.80, whereas FOCUS BilledCost is $4,438.56. We identified a 0.7 pricing conversion rate, so 6,340.80 × 0.7 = $4,438.56. Also, Direct Invoice API has 420 negative credit records totaling -6,340.80, linked to the Atlas Flex Commit contract reference.

We also checked daily cost availability using the FOCUS API. I downloaded the October report through Postman, and we are getting daily usage data up to October 14. For October 14, the BilledCost is around $81.17 across 80 records.

So I think we can use the **FOCUS API for daily cost reporting** and the **Direct Invoice API for credit and invoice comparison**. Next, we need to finalize the remaining column mappings and validate the daily refresh.