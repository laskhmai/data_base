GET https://cloud.mongodb.com/api/atlas/v2/groups/
    {projectId}/processes/{hostId}:{port}/measurements?
    m=CACHE_FILL_RATIO
    &m=DIRTY_FILL_RATIO
    &m=SYSTEM_MEMORY_USED
    &m=SYSTEM_MEMORY_FREE
    &m=SYSTEM_MEMORY_CACHED
    &m=SYSTEM_MEMORY_AVAILABLE
    &m=SWAP_USAGE_USED
    &m=SWAP_IO_IN
    &m=SWAP_IO_OUT
    &granularity=PT5M
    &period=P1D