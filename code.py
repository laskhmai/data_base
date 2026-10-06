FOCUS Field	Current Source in dbo.ActualCost_RR	Assessment	Additional Requirement
BillingAccountId	BillingAccountId / BillingProfileId	Available – lookup/validation required	Confirm agreement-specific mapping
BillingAccountName	BillingAccountName / BillingProfileName	Available – lookup/validation required	Confirm agreement-specific mapping
BillingAccountType	Not directly available	Not confirmed	Agreement/account information
BillingCurrency	BillingCurrency	Available	Validate against Microsoft mapping
BillingPeriodStart	BillingPeriodStartDate	Direct	None
BillingPeriodEnd	BillingPeriodEndDate	Available – transformation required	Apply Microsoft transformation
ChargeCategory	ChargeType, PricingModel	Available – transformation required	Apply Microsoft FOCUS rules
ChargeClass	ChargeType	Available – transformation required	Apply Microsoft FOCUS rules
ChargeDescription	Product	Needs validation	Validate against ProductName semantics
ChargeFrequency	Frequency	Available – transformation required	Apply Microsoft FOCUS rules
ChargePeriodStart	Date	Direct	None
ChargePeriodEnd	Date	Available – transformation required	Derive end date
BilledCost	Cost available; CostInBillingCurrency not present	Needs validation	Actual/Amortized cost source validation
EffectiveCost	Complete source not available in ActualCost_RR	Additional data required	Amortized Cost dataset
ContractedCost	UnitPrice, Quantity	Available – transformation required	Apply pricing calculation
ContractedUnitPrice	UnitPrice	Direct / validate semantics	None if confirmed
ListCost	No confirmed source	Missing	Additional pricing data
ListUnitPrice	No confirmed PayG/List price source	Missing	Additional pricing data
PricingCurrency	No confirmed source	Missing	Additional cost/pricing data
PricingCategory	PricingModel	Available – transformation required	Current population is limited
PricingQuantity	Quantity	Available – transformation required	Pricing-unit rules
PricingUnit	UnitOfMeasure	Available – lookup required	Microsoft pricing-unit reference
ConsumedQuantity	Quantity	Available	Apply applicable FOCUS rule
ConsumedUnit	UnitOfMeasure	Available – lookup/transformation required	Microsoft unit mapping
CommitmentDiscountId	ReservationId available; BenefitId not present	Needs validation	Benefit/Amortized data
CommitmentDiscountName	ReservationName available; BenefitName not present	Needs validation	Benefit/Amortized data
CommitmentDiscountCategory	Benefit information not available	Additional data required	Benefit/Amortized data
CommitmentDiscountType	No confirmed source	Additional data required	Benefit/commitment data
CommitmentDiscountStatus	ChargeType, PricingModel	Not confirmed	Validate with amortized/benefit data
CommitmentDiscountQuantity	No confirmed source	Additional data required	Commitment data
CommitmentDiscountUnit	No confirmed source	Additional data required	Commitment data
CapacityReservationId	AdditionalInfo	Available – transformation required	Extract/validate value
ResourceId	ResourceId	Direct	None
ResourceName	ResourceName	Direct	None
ResourceType	No direct field; may be derived	Lookup required	Microsoft Resource Types reference
ResourceLocation	ResourceLocation	Direct	None
RegionId	ResourceLocation	Available – transformation/lookup required	Microsoft Regions reference
RegionName	ResourceLocation	Lookup required	Microsoft Regions reference
ServiceCategory	ConsumedService / ResourceType	Lookup required	Microsoft Services reference
ServiceName	ConsumedService	Lookup required	Microsoft Services reference
ServiceSubcategory	ConsumedService / ResourceType	Lookup required	Microsoft Services reference
SkuId	ProductId not present	Additional data required	Product/SKU source
SkuPriceId	Meter/pricing information available	Needs validation	Validate Microsoft mapping
SkuMeter	MeterName	Direct	None
ProviderName	Derived constant	Available – derived	Set according to Microsoft rule
PublisherName	PublisherName	Direct	None
InvoiceIssuerName	PublisherName available; PartnerName not present	Needs validation	Partner/invoice information
InvoiceId	Not present	Missing	Additional invoice data
SubAccountId	SubscriptionId	Direct	None
SubAccountName	SubscriptionName	Direct	None
SubAccountType	Derived constant	Available – derived	Apply Microsoft rule
Tags	Tags	Available – transformation required	Convert to FOCUS format


Source fields requiring additional investigation
These are the source-data gaps I would specifically ask Neeraja to check against her Cost Management API/Blob output and the Amortized Cost dataset:
Source Field	Current ActualCost_RR Status	Investigation
BenefitId	Not present	Check API / Amortized dataset
BenefitName	Not present	Check API / Amortized dataset
CostInBillingCurrency	Not present as exact source	Check Actual + Amortized datasets
PayGCostInBillingCurrency	Not present	Check pricing/cost source
BillingCurrencyCode	Exact field not present	Check API/export source
ExchangeRate	Not present	Check API/export source
PartnerName	Not present	Check API/export source
InvoiceId	Not present	Check API/export source
ProductId	Not present	Check API/export source
ProductName	Exact field not present; Product exists	Validate equivalence
ResourceType	Exact field not present	Check source or derive using Microsoft reference data