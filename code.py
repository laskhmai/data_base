-- Search across all schemas
SELECT 
    SCHEMA_NAME(schema_id) AS schema_name,
    name,
    type_desc,
    create_date,
    modify_date
FROM sys.objects
WHERE name LIKE '%Aggregate_Daily_Spend%'
   OR name LIKE '%Aggregate%Spend%'

-- Also check if it exists under a different schema
SELECT ROUTINE_SCHEMA, ROUTINE_NAME
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_NAME LIKE '%Aggregate%Spend%'