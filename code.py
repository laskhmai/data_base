SELECT
    CONVERT(char(7), DateTimeEST, 120) AS MonthKey,
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN StorageMax IS NULL THEN 1 ELSE 0 END) AS EmptyRows,
    SUM(CASE WHEN StorageMax IS NOT NULL THEN 1 ELSE 0 END) AS PopulatedRows,
    CAST(
        100.0 * SUM(CASE WHEN StorageMax IS NULL THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0)
        AS DECIMAL(5,1)
    ) AS PercentEmpty
FROM [Metrics].[SqlDataBasesAggregatedHourly]
GROUP BY CONVERT(char(7), DateTimeEST, 120)
ORDER BY MonthKey;





SELECT
    CAST(DateTimeEST AS date) AS MetricDate,
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN StorageMax IS NULL THEN 1 ELSE 0 END) AS EmptyRows,
    SUM(CASE WHEN StorageMax IS NOT NULL THEN 1 ELSE 0 END) AS PopulatedRows,
    CAST(
        100.0 * SUM(CASE WHEN StorageMax IS NULL THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0)
        AS DECIMAL(5,1)
    ) AS PercentEmpty
FROM [Metrics].[SqlDataBasesAggregatedHourly]
WHERE DateTimeEST >= '2026-07-01'
  AND DateTimeEST <  '2026-09-01'
GROUP BY CAST(DateTimeEST AS date)
ORDER BY MetricDate;