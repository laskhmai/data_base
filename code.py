Hi Neeraja garu,

Yes andi, that is correct.

We only Upsize when CpuMax is consistently high across multiple weeks, not just a single week or single hour spike.

Our logic:
  1. We check CpuMaxP95 per week
  2. If peak value > 80% across most weeks
     → Upsize
  3. If only one week has high CPU
     → NoChange (not Upsize)
     (single spike does not justify upsize)

Example:
  Week 1: CpuMaxP95 = 85%
  Week 2: CpuMaxP95 = 82%
  Week 3: CpuMaxP95 = 90%
  → Consistently high → Upsize ✅

  Week 1: CpuMaxP95 = 85%
  Week 2: CpuMaxP95 = 20%
  Week 3: CpuMaxP95 = 18%
  → Only one spike → NoChange ❌ not Upsize