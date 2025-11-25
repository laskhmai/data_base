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









import re

def looks_like_naming_pattern(val: str) -> bool:
    """
    True for IDs that contain digits but are NOT pure numeric EAPM IDs.
    Examples: 'EAPM-123', 'AA13360', 'id18034'
    """
    if not val:
        return False
    v = val.strip().lower()
    # must contain at least one digit
    if re.search(r'\d', v):
        # but not be a pure numeric EAPM ID
        return not v.isdigit()
    return False

def looks_like_email(val: str) -> bool:
    """
    True for typical email shapes.
    """
    if not val:
        return False
    v = val.strip().lower()
    return "@" in v and "." in v.split("@")[-1]

def looks_like_name(val: str) -> bool:
    """
    True for human-name-like strings (letters + a space).
    """
    if not val:
        return False
    v = val.strip()
    return (" " in v) and any(c.isalpha() for c in v)






if orig_orphan == 1:
    final_id_raw  = row.get("final_app_service_id")
    final_id_norm = normalize_str(final_id_raw)

    # CASE 1: EAPM MATCH FOUND (numeric AND present in Snow)
    if final_id_norm and final_id_norm.isdigit() and final_id_norm in valid_eapm_ids:
        method = "Resource Tags EAPMID"
        confidence = 100
        orphan_reason = None

    # CASE 2: Naming pattern (EAPM-123, AA13360, alphanumeric, email, or name)
    elif (
        looks_like_naming_pattern(final_id_norm)
        or looks_like_email(final_id_norm)
        or looks_like_name(final_id_norm)
    ):
        method = "Virtual Tagging Naming Pattern"
        confidence = 40
        orphan_reason = "invalid"

    # CASE 3: Resource Tag ID match (app / bsn)
    elif billing_id.startswith(("app", "bsn")) or support_id.startswith(("app", "bsn")):
        method = "Virtual Tagging Resource Tag"
        confidence = 60
        orphan_reason = "resource_tag_match"

    # CASE 4: Unmapped
    else:
        method = None
        confidence = 0
        orphan_reason = "NoTag"


elif final_id_norm and (
        final_id_norm.isdigit() or
        re.match(r'^[A-Za-z]+[-]?\d+$', final_id_norm)
    ):
    method = "Virtual Tagging Naming Pattern"
    confidence = 40
    orphan_reason = "invalid"


def insert_batch(batch, insert_sql):
    try:
        with connect(hybridsa1_server, hybridsa1_database,
                     hybridsa1_username, hybridsa1_password) as con:
            cur = con.cursor()
            cur.fast_executemany = True
            cur.executemany(insert_sql, batch)
            con.commit()
        return f"Inserted {len(batch)} rows"
    except Exception as e:
        print("Batch failed, checking rows one by one...", e)
        with connect(hybridsa1_server, hybridsa1_database,
                     hybridsa1_username, hybridsa1_password) as con:
            cur = con.cursor()
            cur.fast_executemany = False
            for i, row in enumerate(batch):
                try:
                    cur.execute(insert_sql, row)
                except Exception as e2:
                    print("❌ Bad row index in DataFrame:", row[0] if row else i)
                    print("Error:", e2)
                    print("Row values:", row)
                    break  # stop at first bad row
        return "Batch contained invalid data"
    









    def clean_types(df):
    # Fix numeric columns
    numeric_cols = [
        "ownership_confidence_score",
        "is_orphaned",
        "is_deleted",
        "has_conflicting_tags"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .fillna(0)
                .astype(str)
                .str.extract(r'(\d+)')   # keep numeric only
                .fillna(0)
                .astype(int)
            )

    # Fix app IDs
    id_cols = [
        "billing_owner_appsvcid",
        "support_owner_appsvcid",
        "billing_owner_appid",
        "support_owner_appid"
    ]

    for col in id_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.extract(r'(\d+)')  # keep only digits (remove EAPM-123 etc.)
                .fillna(0)
                .astype(int)
            )

    return df

