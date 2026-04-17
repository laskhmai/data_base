MongoDB Atlas - SKU Research
User Story 8857416  |  April 2026

1. What are MongoDB SKUs?
MongoDB Atlas clusters come in different sizes called SKUs (Stock Keeping Units). Each SKU has a fixed amount of CPU, RAM, and storage. You pick the SKU based on how much load your database needs to handle.

One thing to keep in mind - each MongoDB Atlas cluster has ONE SKU, not per process. All processes in a cluster share the same instance class (M10, M30, M60, etc.). So when right-sizing, you aggregate all processes first and give ONE recommendation per cluster.

2. Tier Types
There are 3 tier types across providers:

-	Standard - General purpose, balanced CPU and memory
-	Low-CPU - Same memory as Standard but half the vCPUs. Cheaper when CPU is not the bottleneck
-	NVMe - Local NVMe SSD storage, very high IOPS. Available on AWS and Azure only - not supported on GCP

3. What Got Loaded into the DB
Pricing data was manually pulled from the MongoDB Atlas pricing calculator for 3 providers and loaded into Analytics.MongoDBMetaConfig.

Provider	Region	Standard	Low-CPU	NVMe	Total
AWS	us-east-2 (Ohio)	10	7	6	23
Azure	East US 2	8	7	6	21
GCP	us-east4 (N. Virginia)	11	8	0	19
Total	-	29	22	12	63

Azure does not have M140 or M300 in Standard tier.
GCP does not support NVMe.
AWS has M700-low-CPU which Azure and GCP do not.
GCP has M250 (320GB RAM) which AWS and Azure do not have.

4. Database Table
Table created in [Analytics] schema to store SKU pricing data:

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

5. MongoDB Atlas APIs
Two APIs are available to pull SKU and region data programmatically if needed later:

API 1 - Get All Instance Sizes and Regions:
GET https://cloud.mongodb.com/api/atlas/v1/groups/{groupId}/clusters/provider/regions
Returns all providers (AWS/GCP/Azure), instance sizes, available regions per SKU, and default region flags.

API 2 - Get Available MongoDB Versions:
GET https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}/mongoDBVersions
Returns instanceSize, cloudProvider, version, and defaultStatus per SKU.

6. References

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
