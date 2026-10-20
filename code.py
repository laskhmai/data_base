
CREATE TABLE Staging.MongoDB_FOCUS
(
    ChargePeriodStart DATETIME2 NULL,
    SubAccountId NVARCHAR(200) NULL,
    SubAccountName NVARCHAR(300) NULL,
    ResourceId NVARCHAR(300) NULL,
    ResourceType NVARCHAR(200) NULL,
    ServiceName NVARCHAR(300) NULL,
    RegionName NVARCHAR(100) NULL,

    BilledCost DECIMAL(28,10) NULL,
    EffectiveCost DECIMAL(28,10) NULL,

    ChargeCategory NVARCHAR(100) NULL,
    PricingCategory NVARCHAR(100) NULL,
    SkuMeter NVARCHAR(300) NULL,
    ServiceCategory NVARCHAR(200) NULL,
    ServiceSubcategory NVARCHAR(200) NULL
);


CREATE TABLE Staging.MongoDB_DirectInvoice
(
    ChargePeriodStart DATETIME2 NULL,
    SubAccountId NVARCHAR(200) NULL,
    SubAccountName NVARCHAR(300) NULL,
    ResourceId NVARCHAR(300) NULL,
    ResourceType NVARCHAR(200) NULL,
    ServiceName NVARCHAR(300) NULL,
    RegionName NVARCHAR(100) NULL,

    BilledCost DECIMAL(28,10) NULL,
    EffectiveCost DECIMAL(28,10) NULL,

    ChargeCategory NVARCHAR(100) NULL,
    PricingCategory NVARCHAR(100) NULL,
    SkuMeter NVARCHAR(300) NULL,
    ServiceCategory NVARCHAR(200) NULL,
    ServiceSubcategory NVARCHAR(200) NULL,

    -- Additional invoice fields
    InvoiceId NVARCHAR(100) NULL,
    OriginalAmount DECIMAL(28,10) NULL,
    CreditAmount DECIMAL(28,10) NULL
);


