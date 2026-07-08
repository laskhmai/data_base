Hi Neeraja garu,

No problem! I will share the update here.

Previously we had clusters getting NoChange 
even though they were safe to Downsize.

After the code fix, those clusters are now 
correctly getting Downsize recommendations.

For example:
  nlp-prod (M40):
    CpuMaxP95 = 13% (very low)
    Before fix: NoChange ❌
    After fix:  Downsize ✅

  cc-atlas-dev-1 (M10):
    CpuMaxP95 = 31%
    Before fix: NoChange ❌
    After fix:  Downsize ✅

Root cause was: trend detection was marking 
clusters as risky even when CPU values were 
very low (13-35%), because it detected a 
small week-over-week increase.

Fix: We now only consider trend as risky if 
CpuMaxP95 × 2 > 50%. For very low CPU 
clusters the trend is irrelevant.

Could you please verify these look correct 
when you get a chance?

Thank you!