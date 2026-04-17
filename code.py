INSERT INTO [Analytics].[MongoDBMetaConfig]
(SkuName, Tier, vCores, MemorySizeGB, Instance, CostPrHour, Provider, Region)
VALUES
-- =====================
-- AWS - us-east-2
-- =====================
-- Standard
('M10',  'Standard', 2,   2,   'M10',  0.08,  'AWS', 'us-east-2'),
('M20',  'Standard', 2,   4,   'M20',  0.22,  'AWS', 'us-east-2'),  -- ✅ Confirmed
('M30',  'Standard', 2,   8,   'M30',  0.57,  'AWS', 'us-east-2'),
('M40',  'Standard', 4,   16,  'M40',  1.09,  'AWS', 'us-east-2'),
('M50',  'Standard', 8,   32,  'M50',  2.11,  'AWS', 'us-east-2'),
('M60',  'Standard', 16,  64,  'M60',  4.17,  'AWS', 'us-east-2'),
('M80',  'Standard', 32,  128, 'M80',  7.71,  'AWS', 'us-east-2'),
('M140', 'Standard', 48,  192, 'M140', 11.60, 'AWS', 'us-east-2'),
('M200', 'Standard', 64,  256, 'M200', 15.40, 'AWS', 'us-east-2'),
('M300', 'Standard', 96,  384, 'M300', 23.06, 'AWS', 'us-east-2'),
-- Low CPU
('M40',  'Low-CPU', 2,   16,  'M40-low-CPU',  0.82,  'AWS', 'us-east-2'),
('M50',  'Low-CPU', 4,   32,  'M50-low-CPU',  1.56,  'AWS', 'us-east-2'),
('M60',  'Low-CPU', 8,   64,  'M60-low-CPU',  3.08,  'AWS', 'us-east-2'),
('M80',  'Low-CPU', 16,  128, 'M80-low-CPU',  5.92,  'AWS', 'us-east-2'),
('M300', 'Low-CPU', 48,  384, 'M300-low-CPU', 17.56, 'AWS', 'us-east-2'),
('M400', 'Low-CPU', 64,  512, 'M400-low-CPU', 23.64, 'AWS', 'us-east-2'),
('M700', 'Low-CPU', 96,  768, 'M700-low-CPU', 35.10, 'AWS', 'us-east-2'),
-- NVMe
('M40',  'NVMe', 4,   16,  'M40-NVME',  1.35,  'AWS', 'us-east-2'),
('M50',  'NVMe', 8,   32,  'M50-NVME',  2.58,  'AWS', 'us-east-2'),
('M60',  'NVMe', 16,  64,  'M60-NVME',  5.15,  'AWS', 'us-east-2'),
('M80',  'NVMe', 32,  128, 'M80-NVME',  8.28,  'AWS', 'us-east-2'),
('M200', 'NVMe', 64,  256, 'M200-NVME', 15.14, 'AWS', 'us-east-2'),
('M400', 'NVMe', 128, 512, 'M400-NVME', 28.13, 'AWS', 'us-east-2'),

-- =====================
-- Azure - East US 2
-- =====================
-- Standard (no M140 or M300 on Azure)
('M10',  'Standard', 2,   2,   'M10',  0.08,  'Azure', 'East US 2'),
('M20',  'Standard', 2,   4,   'M20',  0.20,  'Azure', 'East US 2'),
('M30',  'Standard', 2,   8,   'M30',  0.54,  'Azure', 'East US 2'),
('M40',  'Standard', 4,   16,  'M40',  1.05,  'Azure', 'East US 2'),
('M50',  'Standard', 8,   32,  'M50',  2.06,  'Azure', 'East US 2'),
('M60',  'Standard', 16,  64,  'M60',  3.93,  'Azure', 'East US 2'),
('M80',  'Standard', 32,  128, 'M80',  7.83,  'Azure', 'East US 2'),
('M200', 'Standard', 64,  256, 'M200', 14.40, 'Azure', 'East US 2'),
-- Low CPU
('M40',  'Low-CPU', 2,   16,  'M40-low-CPU',  0.89,  'Azure', 'East US 2'),
('M50',  'Low-CPU', 4,   32,  'M50-low-CPU',  1.75,  'Azure', 'East US 2'),
('M60',  'Low-CPU', 8,   64,  'M60-low-CPU',  3.30,  'Azure', 'East US 2'),
('M80',  'Low-CPU', 16,  128, 'M80-low-CPU',  6.56,  'Azure', 'East US 2'),
('M200', 'Low-CPU', 32,  256, 'M200-low-CPU', 12.02, 'Azure', 'East US 2'),
('M300', 'Low-CPU', 48,  384, 'M300-low-CPU', 17.55, 'Azure', 'East US 2'),
('M400', 'Low-CPU', 64,  512, 'M400-low-CPU', 21.89, 'Azure', 'East US 2'),
-- NVMe
('M60',  'NVMe', 16,  64,  'M60-NVME',  5.58,  'Azure', 'East US 2'),
('M80',  'NVMe', 32,  128, 'M80-NVME',  10.38, 'Azure', 'East US 2'),
('M200', 'NVMe', 64,  256, 'M200-NVME', 20.67, 'Azure', 'East US 2'),
('M300', 'NVMe', 96,  384, 'M300-NVME', 30.27, 'Azure', 'East US 2'),
('M400', 'NVMe', 128, 512, 'M400-NVME', 39.86, 'Azure', 'East US 2'),  -- ✅ Confirmed
('M600', 'NVMe', 192, 640, 'M600-NVME', 49.46, 'Azure', 'East US 2'),  -- ✅ Confirmed

-- =====================
-- GCP - us-east4
-- =====================
-- Standard
('M10',  'Standard', 2,   2,   'M10',  0.10,  'GCP', 'us-east4'),
('M20',  'Standard', 2,   4,   'M20',  0.22,  'GCP', 'us-east4'),
('M30',  'Standard', 2,   8,   'M30',  0.52,  'GCP', 'us-east4'),
('M40',  'Standard', 4,   16,  'M40',  1.02,  'GCP', 'us-east4'),
('M50',  'Standard', 8,   32,  'M50',  1.97,  'GCP', 'us-east4'),
('M60',  'Standard', 16,  64,  'M60',  3.90,  'GCP', 'us-east4'),
('M80',  'Standard', 32,  128, 'M80',  7.27,  'GCP', 'us-east4'),
('M140', 'Standard', 48,  192, 'M140', 11.00, 'GCP', 'us-east4'),
('M200', 'Standard', 64,  256, 'M200', 15.37, 'GCP', 'us-east4'),
('M250', 'Standard', 80,  320, 'M250', 18.80, 'GCP', 'us-east4'),
('M300', 'Standard', 96,  384, 'M300', 23.00, 'GCP', 'us-east4'),
-- Low CPU
('M40',  'Low-CPU', 2,   16,  'M40-low-CPU',  0.81,  'GCP', 'us-east4'),
('M50',  'Low-CPU', 4,   32,  'M50-low-CPU',  1.56,  'GCP', 'us-east4'),
('M60',  'Low-CPU', 8,   64,  'M60-low-CPU',  3.08,  'GCP', 'us-east4'),
('M80',  'Low-CPU', 16,  128, 'M80-low-CPU',  5.75,  'GCP', 'us-east4'),
('M200', 'Low-CPU', 32,  256, 'M200-low-CPU', 12.12, 'GCP', 'us-east4'),
('M300', 'Low-CPU', 48,  384, 'M300-low-CPU', 18.20, 'GCP', 'us-east4'),
('M400', 'Low-CPU', 64,  512, 'M400-low-CPU', 24.91, 'GCP', 'us-east4'),
('M600', 'Low-CPU', 96,  640, 'M600-low-CPU', 31.81, 'GCP', 'us-east4');
-- Note: GCP does not support NVMe


CREATE TABLE [Analytics].[MongoDBMetaConfig] (
    [Id]             INT IDENTITY(1,1)    NOT NULL,
    [SkuName]        VARCHAR(50)          NOT NULL,
    [Tier]           VARCHAR(50)          NOT NULL,
    [vCores]         INT                  NOT NULL,
    [MemorySizeGB]   INT                  NOT NULL,
    [Instance]       VARCHAR(50)          NOT NULL,
    [CostPrHour]     DECIMAL(10,2)        NOT NULL,
    [Provider]       VARCHAR(20)          NOT NULL,
    [Region]         VARCHAR(50)          NOT NULL,