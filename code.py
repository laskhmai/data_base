Before making any code or output changes, we need to perform a complete end-to-end analysis of the recommendation process.

Please select one affected resource and trace it through every stage:

1. Identify the original source/base tables and show the raw data used for that resource.
2. Identify the silver/intermediate tables and explain every transformation, filter, join, and calculation applied to the data.
3. Identify the gold/target table and show the final values written for Action, Recommended SKU, DTU recommendation, storage recommendation, and estimated savings.
4. Compare the row counts and important column values at every stage to determine exactly where data is being removed, changed, or becoming null.
5. Explain the business logic used to generate RightSize, Terminate, SKU recommendations, and savings.
6. Confirm directly from the target table whether the recommended SKU and savings fields are actually blank or null.
7. Identify the expected output schema and the values accepted by the downstream team. Do not introduce a new value such as “Insufficient Data” without confirming that it is allowed.
8. Document the exact root cause and proposed correction only after completing this analysis.

For now, please do not modify the code or data. First provide the complete source-to-target data lineage, current business logic, findings, and supporting evidence.
