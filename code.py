SELECT 
    clusterid,
    CASE 
        WHEN ReplicationSpecs LIKE '%"diskSizeGB": 0.5%' THEN 'Free'
        ELSE 'Flex'
    END AS TierType
FROM [MongoDB].[Clusters]
WHERE 
    -- Free tier (exactly 0.5 GB)
    ReplicationSpecs LIKE '%"diskSizeGB": 0.5%'
    OR
    -- Flex tier (0.5 to 5.0 GB exactly)
    ReplicationSpecs LIKE '%"diskSizeGB": 1.0%'
    OR ReplicationSpecs LIKE '%"diskSizeGB": 2.0%'
    OR ReplicationSpecs LIKE '%"diskSizeGB": 3.0%'
    OR ReplicationSpecs LIKE '%"diskSizeGB": 4.0%'
    OR ReplicationSpecs LIKE '%"diskSizeGB": 5.0%'
ORDER BY TierType, clusterid;