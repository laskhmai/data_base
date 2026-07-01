Hi Neeraja garu,

After the recommendation logic changes I validated the results against the raw metric tables. Here are 3 sample clusters for your review whenever you get a chance.

You can use the MaxCpuProcessId to verify against the raw tables directly.

1. cgs-prod (M80) → Downsize to M80-LOW-CPU
   AvgCpuMax = 4.68% | PeakCpuMax = 7.11%
   CpuMaxP95×2 = 13.34% (well below 100%)
   Process: atlas-z22n56-shard-00-01.iaovs.mongodb.net:27017
   Savings: $2,908/month

2. cmsonc-eob-prod-cluster (M50) → NoChange
   AvgCpuMax = 34.29% | PeakCpuMax = 83.07%
   CpuMaxP95×2 = 148.79% (risky to downsize)
   Process: atlas-10www-shard-00-02.rtn5p.mongodb.net:27017

3. cwih-cp-mgmt-prod (M50) → Upsize to M60
   AvgCpuMax = 45.15% | PeakCpuMax = 100%
   CpuMaxP95×2 = 200% (overloaded)
   Process: atlas-un771x-shard-00-02.ho8w.mongodb.net:27017

Could you please check if these look correct when you are free?

Thank you!