-- Test COALESCE directly on the exact ClusterKeys used
SELECT 
    ClustersKey,
    Name,
    COALESCE(
        JSON_VALUE(ReplicationSpecs, 
            '$[0].regionConfigs[0].effectiveElectableSpecs.instanceSize'),
        JSON_VALUE(ReplicationSpecs, 
            '$[0].regionConfigs[0].electableSpecs.instanceSize')
    ) AS WhatProcGets
FROM [MongoDB].[Clusters]
WHERE ClustersKey IN (251, 250, 248, 252)