/* Show only the most recent run */
DECLARE @LatestRun datetime2(0) = (
    SELECT MAX(CheckedAt) FROM dbo.daily_update
);

SELECT *
FROM dbo.daily_update
WHERE CheckedAt >= DATEADD(SECOND, -60, @LatestRun)
ORDER BY Status, SchemaName, TableName;