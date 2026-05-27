-- Add ConnectionLimit to MetaConfig
ALTER TABLE [Analytics].[MongoDBMetaConfig]
ADD ConnectionLimit     INT     NULL;
GO

-- Populate it
UPDATE [Analytics].[MongoDBMetaConfig]
SET ConnectionLimit = CASE SkuName
    WHEN 'M0'   THEN 500
    WHEN 'M2'   THEN 500
    WHEN 'M5'   THEN 500
    WHEN 'M10'  THEN 1500
    WHEN 'M20'  THEN 3000
    WHEN 'M30'  THEN 6000
    WHEN 'M40'  THEN 16000
    WHEN 'M50'  THEN 32000
    WHEN 'M60'  THEN 64000
    WHEN 'M80'  THEN 96000
    WHEN 'M140' THEN 96000
    WHEN 'M200' THEN 128000
    WHEN 'M250' THEN 128000
    WHEN 'M300' THEN 128000
    WHEN 'M400' THEN 128000
    WHEN 'M600' THEN 128000
    WHEN 'M700' THEN 128000
    ELSE 16000
END
GO

-- Verify
SELECT SkuName, Tier, Provider, ConnectionLimit
FROM [Analytics].[MongoDBMetaConfig]
ORDER BY ConnectionLimit, Tier
GO