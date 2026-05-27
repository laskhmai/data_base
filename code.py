-- Check 15M table — how many readings per key per hour?
SELECT TOP 20
    [key],
    CAST(DateTime AS DATE)      AS [Date],
    DATEPART(HOUR, DateTime)    AS [Hour],
    COUNT(*)                    AS ReadingsPerHour
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_15M]
WHERE DateTime >= DATEADD(DAY, -1, GETDATE())
GROUP BY
    [key],
    CAST(DateTime AS DATE),
    DATEPART(HOUR, DateTime)
ORDER BY ReadingsPerHour DESC
GO

-- Check 5M table — should have MORE readings
SELECT TOP 20
    [key],
    CAST(DateTime AS DATE)      AS [Date],
    DATEPART(HOUR, DateTime)    AS [Hour],
    COUNT(*)                    AS ReadingsPerHour
FROM [Metrics].[MongoDB_System_Normalized_Cpu_User_Max_5M]
WHERE DateTime >= DATEADD(DAY, -1, GETDATE())
GROUP BY
    [key],
    CAST(DateTime AS DATE),
    DATEPART(HOUR, DateTime)
ORDER BY ReadingsPerHour DESC
GO