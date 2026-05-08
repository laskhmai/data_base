Hey Charles! Found the issue!

dpk-test-cluster does NOT belong 
to cloudea org.

It belongs to:
  Org     : Database Services
  OrgId   : 5dfd2fa5014b763d499389cd
  Project : Dileep-Test1

That's why your code with 
TARGET_ENV = "cloudea" is not 
returning it — you are only 
authenticating against cloudea 
which has 2 clusters.

To get dpk-test-cluster you need 
to authenticate against 
"Database Services" org using:
  mongo-database-services-public-key
  mongo-database-services-private-key

from kv-hybridautomation Key Vault.

If your parking code needs to cover 
ALL orgs, you need to loop through 
all orgs in our KEY_LIST like our 
processor does — not just one 
TARGET_ENV.