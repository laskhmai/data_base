-- Check if any Free or Flex clusters exist in ReplicationSpecs JSON
SELECT 
    clusterid,
    ReplicationSpecs
FROM [MongoDB].[Clusters]
WHERE ReplicationSpecs LIKE '%"M0"%'
   OR ReplicationSpecs LIKE '%"M2"%'
   OR ReplicationSpecs LIKE '%"M5"%'
   OR ReplicationSpecs LIKE '%"instanceSize": "M0"%'
   OR ReplicationSpecs LIKE '%"instanceSize": "M2"%'
   OR ReplicationSpecs LIKE '%"instanceSize": "M5"%';