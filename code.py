-- See every raw reading for shard-00-01
SELECT TOP 50
    DateTime,
    Measurement AS RawMemoryMB
FROM [Metrics].[MongoDB_Memory_Resident_5M]
WHERE [key] = 'atlas-un771x-shard-00-01.ho8iw.mongodb.net:27017'
AND   DateTime >= DATEADD(DAY, -7, GETDATE())
ORDER BY DateTime DESC
GO