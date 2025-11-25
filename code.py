SELECT 
    f.resource_id,
    f.final_app_service_id,
    f.billing_owner_name,
    f.billing_owner_email,
    f.ownership_determination_method,
    f.ownership_confidence_score,
    f.orphan_reason,
    s.EapmId,
    s.AppOwner,
    s.AppOwnerEmail
FROM [AZURE].[Final_Gold_Resources] f
LEFT JOIN [Silver].[SnowNormalizedStaging] s
    ON TRY_CONVERT(int, f.final_app_service_id) = s.EapmId
WHERE f.is_orphaned = 1
  AND f.ownership_determination_method = 'APM via EAPM ID';



SELECT 
    f.resource_id,
    f.final_app_service_id,
    f.billing_owner_name,
    f.billing_owner_email,
    f.ownership_determination_method,
    f.ownership_confidence_score,
    f.orphan_reason,
    s.EapmId
FROM [AZURE].[Final_Gold_Resources] f
LEFT JOIN [Silver].[SnowNormalizedStaging] s
    ON TRY_CONVERT(int, f.final_app_service_id) = s.EapmId
WHERE f.is_orphaned = 1
  AND f.ownership_determination_method = 'APM via Naming Pattern'
  AND f.orphan_reason = 'invalid_eapm_id';



SELECT *
FROM [AZURE].[Final_Gold_Resources]
WHERE is_orphaned = 1
  AND ownership_determination_method = 'APM via Resource Tag ID';




# ---------------------------------------------------------
# FIX: If owner name exists but email is NULL, fill from Snow (EAPM)
# ---------------------------------------------------------

# Build lookup of Name -> Email from EAPM
snow_email_lookup = (
    apps_df[['AppOwner', 'AppOwnerEmail']]
        .dropna()
        .astype(str)
        .apply(lambda x: x.str.strip().str.lower())
        .drop_duplicates()
        .set_index('AppOwner')['AppOwnerEmail']
        .to_dict()
)

# Fix billing owner email
mask_fix_billing = (
    final_df['billing_owner_name'].notna() &
    final_df['billing_owner_name'].astype(str).str.strip().str.lower().isin(snow_email_lookup.keys()) &
    (final_df['billing_owner_email'].isna() | (final_df['billing_owner_email'] == ""))
)

final_df.loc[mask_fix_billing, 'billing_owner_email'] = (
    final_df.loc[mask_fix_billing, 'billing_owner_name']
        .astype(str).str.strip().str.lower()
        .map(snow_email_lookup)
)

# Fix support owner email
mask_fix_support = (
    final_df['support_owner_name'].notna() &
    final_df['support_owner_name'].astype(str).str.strip().str.lower().isin(snow_email_lookup.keys()) &
    (final_df['support_owner_email'].isna() | (final_df['support_owner_email'] == ""))
)

final_df.loc[mask_fix_support, 'support_owner_email'] = (
    final_df.loc[mask_fix_support, 'support_owner_name']
        .astype(str).str.strip().str.lower()
        .map(snow_email_lookup)
)
