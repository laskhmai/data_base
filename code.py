**Title:** Investigate SQL DPU SKU Recommendation and Savings Calculation Issue

**Description:**
The SQL DPU recommendation process completes successfully, and the action is displayed as **RightSize**. However, the recommended SKU and estimated savings are not generated. The same RightSize action also appears for all resources.

Before making code changes, we need to understand the complete process from the foundation level:

* Identify all base/source tables used by the process.
* Understand the data available in each table.
* Identify the columns used for CPU, storage, SKU, and savings calculations.
* Trace the data flow from the source tables through the recommendation logic.
* Review the intermediate data for at least one affected resource.
* Understand how the final action, recommended SKU, and savings are calculated.
* Identify missing, null, filtered, or incorrect data.
* Document any flaws found in the data or existing logic.

**Current Behavior:**

* The process completes without an obvious failure.
* The action defaults to RightSize.
* Recommended SKU is blank or unavailable.
* Estimated savings are blank or unavailable.
* Multiple resources receive the same result.

**Expected Behavior:**

* Each eligible resource should receive an appropriate recommendation based on its utilization data.
* The recommended SKU and estimated savings should be populated.
* Resources should not receive the same default action unless supported by their data.

**Acceptance Criteria:**

1. All base tables and required columns are identified.
2. The end-to-end data and recommendation flow is documented.
3. Input and intermediate data are validated for one affected resource.
4. The exact point where SKU or savings data becomes unavailable is identified.
5. Any problematic filters, null values, or logical conditions are documented.
6. Recommended code or data corrections are proposed only after completing the analysis.
7. No production logic is changed as part of the initial investigation.
