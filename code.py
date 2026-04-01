Hi 

I went through the Metrics Doc and wanted to share my understanding of the task:

The structure is: Org → Projects → Clusters → Processes (Primary + Secondaries).

Since each cluster has ONE SKU, I need to collect metrics for all processes in a cluster, aggregate them, and give ONE right-sizing recommendation per cluster.

The metrics I need to collect per process are:
• CPU → PROCESS_NORMALIZED_CPU_USER
• Memory → MEMORY_RESIDENT
• Disk → DISK_PARTITION_UTILIZATION
• Network → NETWORK_BYTES_IN
• Connections → CONNECTIONS

I tested the Atlas API in Postman using the example ProjectID from the doc and successfully pulled CPU metrics for a process (Status 200 OK). The API flow works:
1. GET /groups → get all projects
2. GET /groups/{projectId}/processes → get all processes
3. GET /processes/{hostId}:{port}/measurements → get metrics

My plan is to write a Python script that loops through all projects and clusters automatically and outputs everything into an our DB with a right-sizing recommendation per cluster.


