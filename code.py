-- Quick test query after deploying proc:
-- #TrueClusterP95 should have 1 row per cluster per hour
-- NOT multiple rows per hour

-- Run this INSIDE the proc as a test (add temporarily):
SELECT
    ClusterKey,
    _date,
    _hour,
    [type],
    businessHour,
    COUNT(*) AS RowCount
FROM #TrueClusterP95
GROUP BY ClusterKey, _date, _hour, [type], businessHour
HAVING COUNT(*) > 1
-- Should return 0 rows ✅

-- Also verify P95 ≠ MAX for most clusters:
SELECT TOP 10
    t.ClusterKey,
    t._date,
    t._hour,
    t.CpuMaxP95,
    f.CpuMaxActual
FROM #TrueClusterP95 t
JOIN (
    SELECT ClusterKey, _date, _hour, [type], businessHour,
           MAX(CpuMax) AS CpuMaxActual
    FROM #FinalMetrics
    GROUP BY ClusterKey, _date, _hour, [type], businessHour
) f ON f.ClusterKey = t.ClusterKey
    AND f._date = t._date
    AND f._hour = t._hour
WHERE t.CpuMaxP95 != f.CpuMaxActual  -- Should find rows now!