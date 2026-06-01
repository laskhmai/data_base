-- Check min and max date in all raw MongoDB metric tables
SELECT 'MongoDB_Memory_Resident_5M'              AS TableName,
       MIN(DateTime) AS MinDate, MAX(DateTime) AS MaxDate,
       COUNT(*)      AS TotalRows
FROM [Metrics].[MongoDB_Memory_Resident_5M]

UNION ALL

SELECT 'MongoDB_System_Normalized_Cpu_User_5M',
       MIN(DateTime), MAX(DateTime), COUNT(*)
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_5M]

UNION ALL

SELECT 'MongoDB_System_Normalized_Cpu_User_Max_5M',
       MIN(DateTime), MAX(DateTime), COUNT(*)
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_5M]

UNION ALL

SELECT 'MongoDB_Connections_15M',
       MIN(DateTime), MAX(DateTime), COUNT(*)
FROM [Metrics].[MongoDB_Connections_15M]

UNION ALL

SELECT 'MongoDB_System_Network_In_5M',
       MIN(DateTime), MAX(DateTime), COUNT(*)
FROM [Metrics].[MongoDB_System_Network_In_5M]

UNION ALL

SELECT 'MongoDB_System_Network_Out_5M',
       MIN(DateTime), MAX(DateTime), COUNT(*)
FROM [Metrics].[MongoDB_System_Network_Out_5M]

UNION ALL

SELECT 'MongoDB_Opcounter_Query_15M',
       MIN(DateTime), MAX(DateTime), COUNT(*)
FROM [Metrics].[MongoDB_Opcounter_Query_15M]

ORDER BY TableName
GO