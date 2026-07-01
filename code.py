GET https://cloud.mongodb.com/api/atlas/v2/groups/
    {projectId}/processes/{hostId}:{port}/measurements?
    m=SYSTEM_MEMORY_PERCENT_USED
    &m=SYSTEM_MEMORY_AVAILABLE
    &m=SYSTEM_MEMORY_FREE
    &m=SYSTEM_MEMORY_TOTAL
    &m=CACHE_FILL_RATIO
    &m=CACHE_USED_BYTES
    &granularity=PT5M
    &period=P1D