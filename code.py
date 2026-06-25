Title:
MongoDB Rightsizing — Validation Document

As a:
COSD Team member

I want to:
Validate that the MongoDB rightsizing
recommendation logic is producing correct
results by comparing raw metric data with
aggregated values, simulated projections
and final recommendations

So that:
Neeraja and leadership can review and
confirm the recommendation model is
accurate before it goes into daily
production scheduling

Acceptance Criteria:

1. Document shows data overview
   - Row counts for all 3 tables
   - Date range covered (May 2026)
   - Zero duplicate records confirmed

2. Document validates 3 known clusters
   - cdr-uat → Downsize ✅
   - consumer-interops-uat → NoChange ✅
   - cwih-cp-mgmt-prod → Upsize ✅

3. Raw metric comparison included
   - Raw CPU per process vs aggregated
   - Raw memory (MB) vs aggregated (%)
   - Raw connections vs aggregated

4. Simulated metrics math verified
   - Projection ratio = 2.0 for all hours
   - CPU and Memory formula confirmed

5. Efficiency columns validated
   - CurrentEfficiency populated ✅
   - WithinEfficiency populated ✅
   - LowCpuEfficiency NULL where
     peak CPU > 50% ✅

6. Cost summary included
   - Net monthly savings = $25,117.44
   - Net annual savings = $301,409.28

Definition of Done:
   Document reviewed and approved
   by Neeraja
   All 3 known clusters validated
   Raw vs aggregated values match
   Document shared with team