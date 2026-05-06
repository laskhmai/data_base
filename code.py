-- Find oldest timestamp across all MongoDB metric tables
SELECT 'MongoDB_System_Normalized_Cpu_User_15M'    AS TableName, MIN(DateTime) AS OldestRecord, MAX(DateTime) AS LatestRecord, COUNT(*) AS TotalRows FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_15M]
UNION ALL
SELECT 'MongoDB_System_Normalized_Cpu_User_Max_15M', MIN(DateTime), MAX(DateTime), COUNT(*) FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_15M]
UNION ALL
SELECT 'MongoDB_Memory_Resident_5M',                 MIN(DateTime), MAX(DateTime), COUNT(*) FROM [Metrics].[MongoDB_Memory_Resident_5M]
UNION ALL
SELECT 'MongoDB_System_Memory_Available_15M',        MIN(DateTime), MAX(DateTime), COUNT(*) FROM [Metrics].[MongoDB_System_Memory_Available_15M]
UNION ALL
SELECT 'MongoDB_System_Network_In_15M',              MIN(DateTime), MAX(DateTime), COUNT(*) FROM [Metrics].[MongoDB_System_Network_In_15M]
UNION ALL
SELECT 'MongoDB_System_Network_In_Max_15M',          MIN(DateTime), MAX(DateTime), COUNT(*) FROM [Metrics].[MongoDB_System_Network_In_Max_15M]
UNION ALL
SELECT 'MongoDB_System_Network_Out_15M',             MIN(DateTime), MAX(DateTime), COUNT(*) FROM [Metrics].[MongoDB_System_Network_Out_15M]
UNION ALL
SELECT 'MongoDB_System_Network_Out_Max_15M',         MIN(DateTime), MAX(DateTime), COUNT(*) FROM [Metrics].[MongoDB_System_Network_Out_Max_15M]
UNION ALL
SELECT 'MongoDB_Network_Num_Requests_15M',           MIN(DateTime), MAX(DateTime), COUNT(*) FROM [Metrics].[MongoDB_Network_Num_Requests_15M]
UNION ALL
SELECT 'MongoDB_Opcounter_Query_15M',                MIN(DateTime), MAX(DateTime), COUNT(*) FROM [Metrics].[MongoDB_Opcounter_Query_15M]
UNION ALL
SELECT 'MongoDB_Opcounter_Insert_15M',               MIN(DateTime), MAX(DateTime), COUNT(*) FROM [Metrics].[MongoDB_Opcounter_Insert_15M]
UNION ALL
SELECT 'MongoDB_Connections_15M',                    MIN(DateTime), MAX(DateTime), COUNT(*) FROM [Metrics].[MongoDB_Connections_15M]
ORDER BY OldestRecord ASC;