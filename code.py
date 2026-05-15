COALESCE(
    JSON_VALUE(cl.ReplicationSpecs,
        '$[0].regionConfigs[0].effectiveElectableSpecs.instanceSize'),
    JSON_VALUE(cl.ReplicationSpecs,
        '$[0].regionConfigs[0].electableSpecs.instanceSize')
) AS InstanceSize,