-- Build discount ratio per SKU + Provider + Region
SELECT
    -- Extract SKU from Spend description
    REPLACE(REPLACE(s.Sku,
        'Atlas ',''),
        ' Instance - Azure','')     AS SkuName,
    'AZURE'                          AS Provider,
    -- Match to MetaConfig
    m.Region,
    ROUND(
        SUM(CASE WHEN s.Sku LIKE '%Instance%'
                 THEN s.Amount   ELSE 0 END)
        /
        NULLIF(SUM(CASE WHEN s.Sku LIKE '%Instance%'
                        THEN s.Quantity ELSE 0 END), 0)
    , 4)                             AS ActualCostPrHour,
    m.CostPrHour                     AS MetaConfigCostPrHour,
    ROUND(
        (SUM(CASE WHEN s.Sku LIKE '%Instance%'
                  THEN s.Amount ELSE 0 END)
        /
        NULLIF(SUM(CASE WHEN s.Sku LIKE '%Instance%'
                        THEN s.Quantity ELSE 0 END), 0))
        / NULLIF(m.CostPrHour, 0)
    , 4)                             AS DiscountRatio
FROM [MongoDB].[Spend] s
JOIN [Analytics].[MongoDBMetaConfig] m
    ON m.Instance = REPLACE(REPLACE(s.Sku,
        'Atlas ',''), ' Instance - Azure','')
WHERE FORMAT(CAST(s.UsageDate AS DATE),'yyyy-MM')
      = '2026-06'
AND   s.Sku LIKE '%Instance%'
AND   m.Provider = 'AZURE'
GROUP BY
    REPLACE(REPLACE(s.Sku,
        'Atlas ',''), ' Instance - Azure',''),
    m.Region,
    m.CostPrHour
ORDER BY SkuName, m.Region
GO