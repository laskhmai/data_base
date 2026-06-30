Hi Neeraja garu,

Made the fixes you mentioned (CpuMax check, 
duplicate fix, cluster table source, Spend 
table, memory flag).

Two things to flag:

1. May data only starts from May 12, not 
   May 1. Missing data for May 1-11.

2. After adding the CpuMax spike check, we 
   are getting 0 Downsize recommendations 
   out of 275 clusters. Even cdr-uat now 
   shows NoChange because of 2 spike hours 
   out of 341 total hours (0.6%).

Questions:
1. Is 20 days of May data fine, or should 
   we backfill May 1-11?
2. Should one rare spike block Downsize for 
   the whole month, or is it ok if spikes 
   are under ~5% of hours?

Thanks!