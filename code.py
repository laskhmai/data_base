1. What are MongoDB SKUs?
MongoDB Atlas clusters come in different sizes called SKUs (Stock Keeping Units). Each SKU has a fixed amount of CPU, RAM, and storage. You pick the SKU based on how much load your database needs to handle.

Each MongoDB Atlas cluster has ONE SKU, not per process. All processes in a cluster share the same instance class (M10, M30, M60 etc.). So when right-sizing, you aggregate all processes first and give ONE recommendation per cluster.

2. Tier Types
There are 3 tier types across providers:

-Standard - General purpose, balanced CPU and memory
-Low-CPU - Same memory as Standard but half the vCPUs. Cheaper when CPU is not the bottleneck
-NVMe - Local NVMe SSD storage, very high IOPS. Available on AWS and Azure only - not supported on GCP

3. How MongoDB Pricing Works
Total price = Compute + Storage (+ extras if used). MongoDB pricing is based on the size of the cluster (e.g. M30), how many nodes it runs, the cloud and region, and how much storage is used. Compute is charged per hour, storage per GB.

3.1  Cluster Tier  (Primary Pricing Driver)
What it controls:
-RAM
-vCPUs
-Baseline performance

Effect on pricing:
-Higher tier = higher fixed hourly cost
-Example: M20 < M30 < M40 < M50

Why it matters:
-RAM must fit the working set
-CPU must handle peak operations

This is ~60-70% of total cost

3.2  Node Count / Replica Set Size
What it controls:
-High availability
-Fault tolerance
-Read scaling

Effect on pricing:
-Cost is per node
-3-node replica set = standard
-5 or 7 nodes = linear cost increase

Formula:
Total Compute Cost = Tier Cost x Number of Nodes

3.3  Cloud Provider
Options:  AWS  |  Azure  |  GCP

Effect on pricing:
-Same tier does not mean same price
-Driven by underlying VM and disk costs

Example:
M30 on AWS  <  M30 on Azure  (in most regions)

Important for enterprise Azure/AWS marketplace deployments

3.4  Region (Geography)
Effect on pricing:
-Infrastructure availability and local taxes
-Compliance regions cost more

Cheapest to Most Expensive (typical):
US East  ->  US West  ->  EU  ->  APAC  ->  Gov/Isolated

Can change cost 15-40% without changing SKU

3.5  Storage Size (GB)
Charged separately from compute.

Effect on pricing:
-Cost per GB per month
-Grows continuously with data

Includes:
-Data
-Indexes (often 20-50% of data size)

3.6  Storage Type / IOPS Tier
SKU options:
-Standard storage
-High-performance / high-IOPS storage

Effect on pricing:
-High-IOPS disks cost significantly more

Use high IOPS only if:
-Heavy write workloads
-Disk-bound queries

3.7  Backup Configuration
Often overlooked but SKU-linked.

Effect on pricing:
-Snapshot storage (GB/month)
-Backup retention period
-Restore traffic

Cost increases with:
Longer retention  +  Larger dataset

Backups can add 20-40% to cluster cost

3.8  Data Transfer (Egress)
Not visible in SKU name, but tied to deployment.

Effect on pricing:
-Charged per GB leaving the cluster
-Cross-region > Internet egress > same-VPC

Key scenarios:
-Multi-region apps
-Analytics exports
-DR replication

3.9  Optional Atlas Features (Feature SKUs)
These are add-on pricing dimensions.

Examples:
-Atlas Search
-Vector Search
-Data Federation
-Online Archive

Effect on pricing:
-Usage-based (index size, queries, data scanned)

Can double cost if enabled without tracking

3.10  Pricing Impact Summary

Factor	Pricing Impact
Cluster tier	Very High
Node count	Very High
Cloud provider	Medium
Region	Medium
Storage size	Medium
Storage type	Medium
Backups	Medium
Data egress	Variable
Features	Variable

One-Sentence Summary:
MongoDB SKU pricing is determined by cluster tier (RAM/CPU), number of nodes, cloud provider, region, storage size and type, backup configuration, data transfer, and enabled Atlas features.

4. Burstable, Free and Flex Tiers

4.1  Burstable
Burstable instances are clusters where the CPU is not fully dedicated — they can burst when needed but are not guaranteed. In MongoDB Atlas, M10, M20 and M30 run on burstable cloud VMs (like AWS T3 instances). They have dedicated RAM but the CPU can burst under load.

SKU	Tier	Why Burstable
M10	Standard	Dedicated RAM but runs on burstable VM — CPU can burst
M20	Standard	Dedicated RAM but runs on burstable VM — CPU can burst
M30	Standard	Dedicated RAM but runs on burstable VM — CPU can burst

M40 and above are fully dedicated fixed CPU — NOT burstable.

In the MetaConfig table, these are identified by updating the Tier column:

UPDATE [Analytics].[MongoDBMetaConfig]
SET Tier = 'Burstable'
WHERE SkuName IN ('M10', 'M20', 'M30');

4.2  Free Tier
Free tier clusters are completely free forever. They run on shared infrastructure with very limited resources. They are used for learning and exploring MongoDB in a cloud environment only — not for production.

SKU	Storage	RAM	vCPU	Cost
M0	512 MB	Shared	Shared	$0.00/hr

How to identify Free tier in the Clusters table — Free clusters have diskSizeGB of exactly 0.5 in the ReplicationSpecs JSON column:

SELECT clusterid, ReplicationSpecs
FROM [MongoDB].[Clusters]
WHERE ReplicationSpecs LIKE '%"diskSizeGB": 0.5%';

4.3  Flex Tier
Flex tier is for application development and testing. Resources and costs scale to your needs. Still runs on shared infrastructure. Costs up to $30/month maximum.

SKU	Storage	RAM	vCPU	Cost
M2/M5	Up to 5 GB	Shared	Shared	$0.011/hr

How to identify Flex tier in the Clusters table — Flex clusters have diskSizeGB between 1.0 and 5.0 in the ReplicationSpecs JSON column:

SELECT clusterid, ReplicationSpecs
FROM [MongoDB].[Clusters]
WHERE ReplicationSpecs LIKE '%"diskSizeGB": 1.0%'
   OR ReplicationSpecs LIKE '%"diskSizeGB": 2.0%'
   OR ReplicationSpecs LIKE '%"diskSizeGB": 3.0%'
   OR ReplicationSpecs LIKE '%"diskSizeGB": 4.0%'
   OR ReplicationSpecs LIKE '%"diskSizeGB": 5.0%';

4.4  Query to Find All Free and Flex Clusters
Run this query to check if any Free or Flex clusters exist in your actual cluster data:

SELECT
    clusterid,
    CASE
        WHEN ReplicationSpecs LIKE '%"diskSizeGB": 0.5%' THEN 'Free'
        ELSE 'Flex'
    END AS TierType
FROM [MongoDB].[Clusters]
WHERE
    ReplicationSpecs LIKE '%"diskSizeGB": 0.5%'   -- Free (512MB)
    OR ReplicationSpecs LIKE '%"diskSizeGB": 1.0%' -- Flex
    OR ReplicationSpecs LIKE '%"diskSizeGB": 2.0%' -- Flex
    OR ReplicationSpecs LIKE '%"diskSizeGB": 3.0%' -- Flex
    OR ReplicationSpecs LIKE '%"diskSizeGB": 4.0%' -- Flex
    OR ReplicationSpecs LIKE '%"diskSizeGB": 5.0%' -- Flex
ORDER BY TierType, clusterid;

Note: diskSizeGB is stored inside the ReplicationSpecs JSON column in the Clusters table. We use LIKE with exact decimal values (0.5, 1.0 etc) to avoid matching larger sizes like 128.0 or 1024.0.

5. What Got Loaded into the DB
4. What Got Loaded into the DB
The Analytics.MongoDBMetaConfig table was created by Jayanth Paluri as part of this user story. All SKU pricing data was manually researched by going through the MongoDB Atlas pricing calculator for each provider, tier, and instance size one by one, and then inserted into the table.

Pricing data was pulled for 3 providers and loaded into Analytics.MongoDBMetaConfig.

Provider	Region	Standard	Low-CPU	NVMe	Total
AWS	us-east-2 (Ohio)	10	7	6	23
Azure	East US 2	8	7	6	21
GCP	us-east4 (N. Virginia)	11	8	0	19
Total	-	29	22	12	63

Azure does not have M140 or M300 in Standard tier.
GCP does not support NVMe.
AWS has M700-low-CPU which Azure and GCP do not have.
GCP has M250 (320GB RAM) which AWS and Azure do not have.
Free and Flex tiers are not included - these are shared/serverless and not relevant for right-sizing.

5. Database Table
Table created by Jayanth Paluri in [Analytics] schema to store MongoDB Atlas SKU pricing reference data. This table is used for cost optimization and right-sizing recommendations.

CREATE TABLE [Analytics].[MongoDBMetaConfig]
(
    Id              INT IDENTITY(1,1)  NOT NULL,
    SkuName         VARCHAR(50)        NOT NULL,
    Tier            VARCHAR(50)        NOT NULL,   -- Standard / Low-CPU / NVMe
    vCores          INT                NOT NULL,
    MemorySizeGB    INT                NOT NULL,
    Instance        VARCHAR(50)        NOT NULL,
    CostPrHour      DECIMAL(10,2)      NOT NULL,
    Provider        VARCHAR(20)        NOT NULL,   -- AWS / Azure / GCP
    Region          VARCHAR(50)        NOT NULL,
    CreatedDate     DATETIME           NOT NULL  DEFAULT GETDATE(),
    IsActive        BIT                NOT NULL  DEFAULT 1
);

6. MongoDB Atlas APIs
Two APIs are available to pull SKU and region data programmatically if needed later:

API 1 - Get All Instance Sizes and Regions:
GET https://cloud.mongodb.com/api/atlas/v1/groups/{groupId}/clusters/provider/regions
Returns all providers (AWS/GCP/Azure), instance sizes, available regions per SKU, and default region flags.

API 2 - Get Available MongoDB Versions:
GET https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}/mongoDBVersions
Returns instanceSize, cloudProvider, version, and defaultStatus per SKU.

7. References

Official MongoDB Pricing:
https://www.mongodb.com/pricing
https://www.mongodb.com/pricing/calculator

Instance Size Reference Docs:
AWS:   https://www.mongodb.com/docs/atlas/reference/amazon-aws/
Azure: https://www.mongodb.com/docs/atlas/reference/microsoft-azure/
GCP:   https://www.mongodb.com/docs/atlas/reference/google-gcp/

Atlas Admin API:
https://www.mongodb.com/docs/api/doc/atlas-admin-api-v2/
https://www.mongodb.com/docs/atlas/billing/cluster-configuration-costs/

Cluster Sizing Guide:
https://www.mongodb.com/docs/atlas/sizing-tier-selection/