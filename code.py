import pyodbc


conn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server};'
    f'DATABASE={database};UID={username};PWD={password};'
)
cursor = conn.cursor()

# Drop and recreate
cursor.execute("DROP TABLE IF EXISTS [Analytics].[MongoDBMetaConfig]")
cursor.execute("""
CREATE TABLE [Analytics].[MongoDBMetaConfig]
(
    SkuName      VARCHAR(50)   NOT NULL,
    Tier         VARCHAR(50)   NOT NULL,
    vCores       INT           NOT NULL,
    MemorySizeGB INT           NOT NULL,
    Instance     VARCHAR(50)   NOT NULL,
    CostPrHour   DECIMAL(10,2) NOT NULL,
    Provider     VARCHAR(20)   NOT NULL,
    Region       VARCHAR(50)   NOT NULL
)
""")
conn.commit()

# All rows as list
rows = [
    # AWS | US_EAST_2 | Standard
    ('M40','Standard',4,  16, 'M40', 1.09,'AWS','US_EAST_2'),
    ('M50','Standard',8,  32, 'M50', 2.11,'AWS','US_EAST_2'),
    ('M60','Standard',16, 64, 'M60', 4.17,'AWS','US_EAST_2'),
    ('M80','Standard',32, 128,'M80', 7.71,'AWS','US_EAST_2'),
    ('M200','Standard',64,256,'M200',15.40,'AWS','US_EAST_2'),
    ('M300','Standard',96,384,'M300',23.06,'AWS','US_EAST_2'),
    # AWS | US_EAST_2 | Low-CPU
    ('M40','Low-CPU',2,  16, 'M40-low-CPU', 0.82,'AWS','US_EAST_2'),
    ('M50','Low-CPU',4,  32, 'M50-low-CPU', 1.56,'AWS','US_EAST_2'),
    ('M60','Low-CPU',8,  64, 'M60-low-CPU', 3.08,'AWS','US_EAST_2'),
    ('M80','Low-CPU',16, 128,'M80-low-CPU', 5.92,'AWS','US_EAST_2'),
    ('M400','Low-CPU',64,512,'M400-low-CPU',23.64,'AWS','US_EAST_2'),
    ('M700','Low-CPU',96,768,'M700-low-CPU',35.10,'AWS','US_EAST_2'),
    # AWS | US_EAST_2 | NVMe
    ('M40','NVMe',4,  16, 'M40-NVME', 1.35,'AWS','US_EAST_2'),
    ('M50','NVMe',8,  32, 'M50-NVME', 2.58,'AWS','US_EAST_2'),
    ('M60','NVMe',16, 64, 'M60-NVME', 5.15,'AWS','US_EAST_2'),
    ('M80','NVMe',32, 128,'M80-NVME', 8.28,'AWS','US_EAST_2'),
    ('M200','NVMe',64,256,'M200-NVME',15.14,'AWS','US_EAST_2'),
    ('M400','NVMe',128,512,'M400-NVME',28.13,'AWS','US_EAST_2'),
    # AWS | US_EAST_2 | Burstable
    ('M10','Burstable',2,2,'M10',0.08,'AWS','US_EAST_2'),
    ('M20','Burstable',2,4,'M20',0.22,'AWS','US_EAST_2'),
    ('M30','Burstable',2,8,'M30',0.57,'AWS','US_EAST_2'),
    # AWS | US_EAST_2 | Free
    ('M0','Free',2,1,'M0',0.00,'AWS','US_EAST_2'),
    # AZURE | US_EAST_2 | Standard
    ('M40','Standard',4,  16, 'M40', 1.05,'AZURE','US_EAST_2'),
    ('M50','Standard',8,  32, 'M50', 2.06,'AZURE','US_EAST_2'),
    ('M60','Standard',16, 64, 'M60', 3.93,'AZURE','US_EAST_2'),
    ('M80','Standard',32, 128,'M80', 7.83,'AZURE','US_EAST_2'),
    ('M200','Standard',64,256,'M200',14.40,'AZURE','US_EAST_2'),
    ('M400','Standard',128,512,'M400',39.86,'AZURE','US_EAST_2'),
    # AZURE | US_EAST_2 | Low-CPU
    ('M40','Low-CPU',2,  16, 'M40-low-CPU', 0.89,'AZURE','US_EAST_2'),
    ('M50','Low-CPU',4,  32, 'M50-low-CPU', 1.75,'AZURE','US_EAST_2'),
    ('M60','Low-CPU',8,  64, 'M60-low-CPU', 3.30,'AZURE','US_EAST_2'),
    ('M80','Low-CPU',16, 128,'M80-low-CPU', 6.56,'AZURE','US_EAST_2'),
    ('M200','Low-CPU',32,256,'M200-low-CPU',12.02,'AZURE','US_EAST_2'),
    ('M300','Low-CPU',48,384,'M300-low-CPU',17.55,'AZURE','US_EAST_2'),
    ('M400','Low-CPU',64,512,'M400-low-CPU',21.89,'AZURE','US_EAST_2'),
    # AZURE | US_EAST_2 | NVMe
    ('M60','NVMe',16, 64, 'M60-NVME', 5.58,'AZURE','US_EAST_2'),
    ('M80','NVMe',32, 128,'M80-NVME', 10.38,'AZURE','US_EAST_2'),
    ('M200','NVMe',64,256,'M200-NVME',20.67,'AZURE','US_EAST_2'),
    ('M300','NVMe',96,384,'M300-NVME',30.27,'AZURE','US_EAST_2'),
    ('M400','NVMe',128,512,'M400-NVME',39.86,'AZURE','US_EAST_2'),
    ('M600','NVMe',192,640,'M600-NVME',49.46,'AZURE','US_EAST_2'),
    # AZURE | US_EAST_2 | Burstable
    ('M10','Burstable',2,2,'M10',0.08,'AZURE','US_EAST_2'),
    ('M20','Burstable',2,4,'M20',0.20,'AZURE','US_EAST_2'),
    ('M30','Burstable',2,8,'M30',0.54,'AZURE','US_EAST_2'),
    # AZURE | US_EAST_2 | Free
    ('M0','Free',2,1,'M0',0.00,'AZURE','US_EAST_2'),
    # AZURE | US_CENTRAL | Standard
    ('M40','Standard',4,  16,'M40', 1.05,'AZURE','US_CENTRAL'),
    ('M50','Standard',8,  32,'M50', 2.06,'AZURE','US_CENTRAL'),
    ('M60','Standard',16, 64,'M60', 3.93,'AZURE','US_CENTRAL'),
    ('M80','Standard',32,128,'M80', 7.83,'AZURE','US_CENTRAL'),
    ('M200','Standard',64,256,'M200',14.40,'AZURE','US_CENTRAL'),
    # AZURE | US_CENTRAL | Low-CPU
    ('M40','Low-CPU',2, 16,'M40-low-CPU',0.89,'AZURE','US_CENTRAL'),
    ('M50','Low-CPU',4, 32,'M50-low-CPU',1.75,'AZURE','US_CENTRAL'),
    ('M60','Low-CPU',8, 64,'M60-low-CPU',3.30,'AZURE','US_CENTRAL'),
    ('M80','Low-CPU',16,128,'M80-low-CPU',6.56,'AZURE','US_CENTRAL'),
    # AZURE | US_CENTRAL | Burstable
    ('M10','Burstable',2,2,'M10',0.08,'AZURE','US_CENTRAL'),
    ('M20','Burstable',2,4,'M20',0.20,'AZURE','US_CENTRAL'),
    ('M30','Burstable',2,8,'M30',0.54,'AZURE','US_CENTRAL'),
    # AZURE | US_CENTRAL | Free
    ('M0','Free',2,1,'M0',0.00,'AZURE','US_CENTRAL'),
    # GCP | US_EAST_4 | Standard
    ('M40','Standard',4,  16, 'M40', 1.02,'GCP','US_EAST_4'),
    ('M50','Standard',8,  32, 'M50', 1.97,'GCP','US_EAST_4'),
    ('M60','Standard',16, 64, 'M60', 3.90,'GCP','US_EAST_4'),
    ('M80','Standard',32, 128,'M80', 7.27,'GCP','US_EAST_4'),
    ('M140','Standard',48,192,'M140',11.00,'GCP','US_EAST_4'),
    ('M200','Standard',64,256,'M200',15.37,'GCP','US_EAST_4'),
    ('M250','Standard',80,320,'M250',18.80,'GCP','US_EAST_4'),
    ('M300','Standard',96,384,'M300',23.00,'GCP','US_EAST_4'),
    ('M400','Standard',128,512,'M400',24.91,'GCP','US_EAST_4'),
    # GCP | US_EAST_4 | Low-CPU
    ('M40','Low-CPU',2,  16, 'M40-low-CPU', 0.81,'GCP','US_EAST_4'),
    ('M50','Low-CPU',4,  32, 'M50-low-CPU', 1.56,'GCP','US_EAST_4'),
    ('M60','Low-CPU',8,  64, 'M60-low-CPU', 3.08,'GCP','US_EAST_4'),
    ('M80','Low-CPU',16, 128,'M80-low-CPU', 5.75,'GCP','US_EAST_4'),
    ('M200','Low-CPU',32,256,'M200-low-CPU',12.12,'GCP','US_EAST_4'),
    ('M300','Low-CPU',48,384,'M300-low-CPU',18.20,'GCP','US_EAST_4'),
    ('M400','Low-CPU',64,512,'M400-low-CPU',24.91,'GCP','US_EAST_4'),
    ('M600','Low-CPU',96,640,'M600-low-CPU',31.81,'GCP','US_EAST_4'),
    # GCP | US_EAST_4 | Burstable
    ('M10','Burstable',2,2,'M10',0.10,'GCP','US_EAST_4'),
    ('M20','Burstable',2,4,'M20',0.22,'GCP','US_EAST_4'),
    ('M30','Burstable',2,8,'M30',0.52,'GCP','US_EAST_4'),
    # GCP | US_EAST_4 | Free
    ('M0','Free',2,1,'M0',0.00,'GCP','US_EAST_4'),
    # GCP | CENTRAL_US | Standard
    ('M40','Standard',4,  16, 'M40', 1.02,'GCP','CENTRAL_US'),
    ('M50','Standard',8,  32, 'M50', 1.97,'GCP','CENTRAL_US'),
    ('M60','Standard',16, 64, 'M60', 3.90,'GCP','CENTRAL_US'),
    ('M80','Standard',32, 128,'M80', 7.27,'GCP','CENTRAL_US'),
    ('M140','Standard',48,192,'M140',11.00,'GCP','CENTRAL_US'),
    ('M200','Standard',64,256,'M200',15.37,'GCP','CENTRAL_US'),
    ('M300','Standard',96,384,'M300',23.00,'GCP','CENTRAL_US'),
    ('M400','Standard',128,512,'M400',24.91,'GCP','CENTRAL_US'),
    # GCP | CENTRAL_US | Low-CPU
    ('M40','Low-CPU',2, 16,'M40-low-CPU', 0.81,'GCP','CENTRAL_US'),
    ('M50','Low-CPU',4, 32,'M50-low-CPU', 1.56,'GCP','CENTRAL_US'),
    ('M60','Low-CPU',8, 64,'M60-low-CPU', 3.08,'GCP','CENTRAL_US'),
    ('M80','Low-CPU',16,128,'M80-low-CPU', 5.75,'GCP','CENTRAL_US'),
    ('M200','Low-CPU',32,256,'M200-low-CPU',12.12,'GCP','CENTRAL_US'),
    # GCP | CENTRAL_US | Burstable
    ('M10','Burstable',2,2,'M10',0.10,'GCP','CENTRAL_US'),
    ('M20','Burstable',2,4,'M20',0.22,'GCP','CENTRAL_US'),
    ('M30','Burstable',2,8,'M30',0.52,'GCP','CENTRAL_US'),
    # GCP | CENTRAL_US | Free
    ('M0','Free',2,1,'M0',0.00,'GCP','CENTRAL_US'),
    # GCP | EASTERN_US | Standard
    ('M40','Standard',4,  16,'M40', 1.02,'GCP','EASTERN_US'),
    ('M50','Standard',8,  32,'M50', 1.97,'GCP','EASTERN_US'),
    ('M60','Standard',16, 64,'M60', 3.90,'GCP','EASTERN_US'),
    ('M80','Standard',32,128,'M80', 7.27,'GCP','EASTERN_US'),
    # GCP | EASTERN_US | Burstable
    ('M10','Burstable',2,2,'M10',0.10,'GCP','EASTERN_US'),
    ('M20','Burstable',2,4,'M20',0.22,'GCP','EASTERN_US'),
    ('M30','Burstable',2,8,'M30',0.52,'GCP','EASTERN_US'),
    # GCP | EASTERN_US | Free
    ('M0','Free',2,1,'M0',0.00,'GCP','EASTERN_US'),
]

sql = """
INSERT INTO [Analytics].[MongoDBMetaConfig]
(SkuName,Tier,vCores,MemorySizeGB,Instance,CostPrHour,Provider,Region)
VALUES (?,?,?,?,?,?,?,?)
"""

cursor.executemany(sql, rows)
conn.commit()
cursor.close()
conn.close()

print(f"Inserted {len(rows)} rows successfully!")