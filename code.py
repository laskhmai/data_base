Perform a read-only analysis of the target table [Metrics].[SQLDbDTURecommendationEfficiencySavings]. Do not modify any data or code.

First, show the count of records for each distinct Action value, such as RightSize, Optimal, Terminate, and No Metrics, grouped by month.

Then select one representative record for each Action from a healthy previous month and from August 2026. Display all important fields, including:

Month
Resource ID and Resource Name
Current SKU
DayType and HourType
DtuRec
StgRec
OverallWithinFamily
OverallOutsideFamily
Action
Comment
Spend30days
WithinFamilySavings
OutsideFamilySavings

Also include separate examples for these variations if they exist:

RightSize with a valid recommended SKU
RightSize with blank recommendations
Optimal
Terminate
No Metrics
Records with calculated savings
Records with blank or zero savings

Present the results in a clear comparison table and explain how a valid recommendation differs from the incorrect August RightSize fallback. Use only SELECT queries and do not run any INSERT, UPDATE, DELETE, or stored procedures.