Enhance MongoDB rightsizing recommendations 
to address edge cases identified during 
review of initial recommendations.

Current implementation covers CPU and 
Connections basic logic. This story covers 
the following enhancements:

1. Add CpuMaxP95 column to recommendations 
   table for better traceability of why 
   a recommendation was given

2. Fix connection limit calculation for 
   sharded clusters (multiply per-shard 
   limit by number of shards)

3. Fix misleading comments — when connections 
   are intensive but recommendation is 
   Downsize or NoChange, comment should 
   reflect actual reason for recommendation

4. Replace AvgCpuMax column with CpuAvgP95 
   which is the actual value used in logic

5. Add memory recommendations once memory 
   metric approach is confirmed

6. Fix Spend column — currently showing SKU 
   cost calculation, should use actual spend 
   from MongoDB.Spend table (invoice generated 
   2nd of every month)

7. consumer-interops-uat specific case — 
   investigate and verify recommendation 
   is correct

8. Investigate 37 borderline clusters 
   currently showing NoChange with 
   CpuMaxP95 between 25-50%