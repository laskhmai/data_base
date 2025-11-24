import pandas as pd
from typing import Optional
import hashlib


# ---------- shared helpers ----------

def normalize_str(value):
    """Lower-case, strip, None-safe string."""
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def pick_app_service_id(row: pd.Series) -> Optional[str]:
    """
    For non-orphan rows: extract AppServiceID from BillingOwnerAppsvcid / SupportOwnerAppsvcid.
    Returns the first valid value starting with 'app' or 'bsn'.
    """
    for col in ["BillingOwnerAppsvcid", "SupportOwnerAppsvcid"]:
        val = row.get(col)
        if isinstance(val, str):
            v = val.strip()
            if len(v) > 3 and v[:3].lower() in ("app", "bsn"):
                return v
    return None


def transform(resources_df: pd.DataFrame,
              inferred_df: pd.DataFrame,
              apps_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms resource, inferred-tag, and Snow app metadata into the Final_Gold style DataFrame.
    Includes:
      - ownership determination
      - confidence scoring
      - orphan flag and orphan_reason
      - enrichment from tags + Snow
    """

    # -------------- configuration --------------

    # strings that mean "invalid" / "not real owner"
    invalid_ids = {
        "unknown/orphaned",
        "bill to subscription owner",
        "needs update",
        "test",
        "orphaned",
        "none",
    }

    # columns we want to null out when the final_app_service_id is invalid
    cols_to_null_when_invalid = [
        "billing_owner_appsvcid",
        "support_owner_appsvcid",
        "application_name",
        "billing_owner_name",
        "support_owner_name",
        "billing_owner_email",
        "support_owner_email",
        "business_unit",
        "department",
    ]

    # -------------- copy inputs --------------

    res = resources_df.copy()
    inf = inferred_df.copy()
    apps = apps_df.copy()

    # -------------- normalize keys --------------

    # resource key (Azure resourceId)
    res["resource_id_key"] = res["ResourceId"].astype(str).str.lower()
    inf["resource_id_key"] = inf["resource_id"].astype(str).str.lower()

    # Snow: build a single key that can match either AppServiceID or AppID
    apps_by_service = apps.copy()
    apps_by_service["appserviceid_key"] = apps_by_service["AppServiceID"].astype(str).str.lower()

    apps_by_appid = apps.copy()
    apps_by_appid["appserviceid_key"] = apps_by_appid["AppID"].astype(str).str.lower()

    apps_all = pd.concat([apps_by_service, apps_by_appid], ignore_index=True)
    apps_all = apps_all.drop_duplicates(subset=["appserviceid_key"], keep="first")

    # -------------- split orphan / non-orphan --------------

    is_orphan_mask = res["IsOrphaned"].fillna(0).astype(int).eq(1)
    non_orphaned_res = res[~is_orphan_mask].copy()
    orphaned_res = res[is_orphan_mask].copy()

    # -------------- non-orphan: join tags + choose AppSvc --------------

    # tags we care about from inferred table
    tag_cols = [
        "resource_id_key",
        "app_service_id",
        "app_id",
        "inferred_app_name",
        "ml_app_name",
        "app_owner",
        "ml_app_owner",
        "app_pattern",
        "app_name_source",
        "confidence_score",
        "rg_inferred_app_name",
        "res_tags",
    ]

    non_orphan_with_tags = non_orphaned_res.merge(
        inf[tag_cols],
        on="resource_id_key",
        how="left",
        suffixes=("", "_tag"),
    )

    # primary appservice from 4.1 (for non-orphan we trust this more)
    non_orphan_with_tags["primary_appservice_4_1"] = non_orphan_with_tags.apply(
        pick_app_service_id, axis=1
    )

    # choose final AppSvc for non-orphan
    def pick_non_orphaned_appsvc(row: pd.Series) -> Optional[str]:
        primary = row.get("primary_appservice_4_1")
        primary_norm = normalize_str(primary)

        # 1) primary from 4.1, if valid
        if primary_norm and primary_norm not in invalid_ids:
            return str(primary).strip()

        # 2) tags app_service_id if present and valid
        tag_appsvc = row.get("app_service_id")
        tag_norm = normalize_str(tag_appsvc)
        if tag_norm and tag_norm not in invalid_ids:
            return str(tag_appsvc).strip()

        # 3) nothing usable
        return None

    non_orphan_with_tags["final_app_service_id"] = non_orphan_with_tags.apply(
        pick_non_orphaned_appsvc, axis=1
    )

    # -------------- orphan: join tags + AppSvc/AppID fallback --------------

    orphan_tags = orphaned_res.merge(
        inf[tag_cols],
        on="resource_id_key",
        how="left",
        suffixes=("", "_tag"),
    )

    # primary (if any) – same helper as non-orphan side
    orphan_tags["primary_appservice_4_1"] = orphan_tags.apply(
        pick_app_service_id, axis=1
    )

    def pick_orphaned_appsvc(row: pd.Series) -> Optional[str]:
        """
        For orphan rows:
        1) use primary_appservice_4_1 if valid and not in invalid_ids
        2) else use app_service_id from tags if valid
        3) else use app_id from tags if it starts with 'app' and not in invalid_ids
        4) else None
        """
        # 1. primary from 4.1
        primary = row.get("primary_appservice_4_1")
        primary_norm = normalize_str(primary)
        if primary_norm and primary_norm not in invalid_ids:
            return str(primary).strip()

        # 2. app_service_id from tags
        tag_appsvc = row.get("app_service_id")
        tag_appsvc_norm = normalize_str(tag_appsvc)
        if tag_appsvc_norm and tag_appsvc_norm not in invalid_ids:
            return str(tag_appsvc).strip()

        # 3. fallback to app_id from tags if looks like a real app id
        tag_appid = row.get("app_id")
        tag_appid_norm = normalize_str(tag_appid)
        if tag_appid_norm.startswith("app") and tag_appid_norm not in invalid_ids:
            # e.g., this is where 18034 etc. becomes the "final_app_service_id"
            return str(tag_appid).strip()

        # 4. nothing usable
        return None

    # *** this must be defined BEFORE we reference "final_app_service_id" anywhere else ***
    orphan_tags["final_app_service_id"] = orphan_tags.apply(pick_orphaned_appsvc, axis=1)

    # for orphaned rows, also fill AppSvc owner columns directly from this id
    orphan_tags["BillingOwnerAppsvcid"] = orphan_tags["final_app_service_id"]
    orphan_tags["SupportOwnerAppsvcid"] = orphan_tags["final_app_service_id"]

    # -------------- infer appsvc source (resource table vs tags table) --------------

    def infer_appsvc_source_non_orphan(row: pd.Series) -> str:
        primary = normalize_str(row.get("primary_appservice_4_1") or "")
        tag_val = normalize_str(row.get("app_service_id") or "")
        fa = normalize_str(row.get("final_app_service_id") or "")

        if fa and fa == primary:
            return "resource table"
        if fa and fa == tag_val:
            return "tags_table"
        return "none"

    non_orphan_with_tags["appsvc_source"] = non_orphan_with_tags.apply(
        infer_appsvc_source_non_orphan, axis=1
    )

    def infer_appsvc_source_orphan(row: pd.Series) -> str:
        primary = normalize_str(row.get("primary_appservice_4_1") or "")
        tag_val = normalize_str(row.get("app_service_id") or "")
        fa = normalize_str(row.get("final_app_service_id") or "")

        if fa and fa == primary:
            return "resource table"
        if fa and fa == tag_val:
            return "tags_table"
        return "none"

    orphan_tags["appsvc_source"] = orphan_tags.apply(
        infer_appsvc_source_orphan, axis=1
    )

    # -------------- join with Snow on final_app_service_id --------------

    non_orphan_with_tags["appserviceid_key"] = (
        non_orphan_with_tags["final_app_service_id"].astype(str).str.lower()
    )
    orphan_tags["appserviceid_key"] = (
        orphan_tags["final_app_service_id"].astype(str).str.lower()
    )

    non_orphan_join = non_orphan_with_tags.merge(
        apps_all,
        left_on="appserviceid_key",
        right_on="appserviceid_key",
        how="left",
        suffixes=("", "_snow"),
    )
    non_orphan_join["ownership_path"] = "non_orphaned_snow"

    orphan_join = orphan_tags.merge(
        apps_all,
        left_on="appserviceid_key",
        right_on="appserviceid_key",
        how="left",
        suffixes=("", "_snow"),
    )
    orphan_join["ownership_path"] = "orphaned_tags_snow"

    # -------------- combine both sets --------------

    common_cols = list(set(non_orphan_join.columns).intersection(orphan_join.columns))
    final_df = pd.concat(
        [non_orphan_join[common_cols], orphan_join[common_cols]],
        ignore_index=True,
    )

    # -------------- add / combine ownership columns --------------

    # AppID: Snow AppID + tags app_id
    final_df["AppID"] = final_df["AppID"].combine_first(final_df["app_id"])

    # Billing / Support AppSvc
    final_df["billing_owner_appsvcid"] = final_df["BillingOwnerAppsvcid"]
    final_df["support_owner_appsvcid"] = final_df["SupportOwnerAppsvcid"]

    # application_name: prefer real app names in this order
    if "application_name" not in final_df.columns:
        final_df["application_name"] = None

    final_df["application_name"] = (
        final_df["AppName"]
        .combine_first(final_df["inferred_app_name"])
        .combine_first(final_df["ml_app_name"])
        .combine_first(final_df["rg_inferred_app_name"])
    )

    # owner name/email: pick from Snow → tags
    def pick_owner_name(row: pd.Series) -> Optional[str]:
        for col in ["AppOwner", "app_owner", "ml_app_owner"]:
            v = row.get(col)
            if pd.notna(v) and str(v).strip():
                return str(v).strip()
        return None

    def pick_owner_email(row: pd.Series) -> Optional[str]:
        v = row.get("AppOwnerEmail")
        if pd.notna(v) and str(v).strip():
            return str(v).strip()
        return None

    final_df["billing_owner_name"] = final_df.apply(pick_owner_name, axis=1)
    final_df["support_owner_name"] = final_df["billing_owner_name"]

    final_df["billing_owner_email"] = final_df.apply(pick_owner_email, axis=1)
    final_df["support_owner_email"] = final_df["billing_owner_email"]

    # business unit & department
    final_df["business_unit"] = final_df.get("BusinessUnit")
    final_df["department"] = final_df.get("Department")

    # -------------- platform team name --------------

    def platform_team(row: pd.Series) -> Optional[str]:
        val = normalize_str(row.get("IsPlatformManaged", ""))
        if val == "true":
            # when platform-managed, platform team is support_owner_name
            return row.get("support_owner_name") or ""
        return None

    final_df["platform_team_name"] = final_df.apply(platform_team, axis=1)

    # -------------- keep original orphan flag --------------

    final_df["original_is_orphaned"] = final_df["IsOrphaned"].fillna(0).astype(int)

    # -------------- ownership method / confidence / orphan_reason --------------

    def compute_ownership_fields(row: pd.Series) -> pd.Series:
        orig_orphan = int(row.get("original_is_orphaned") or 0)
        final_id_raw = row.get("final_app_service_id")
        final_id = normalize_str(final_id_raw)

        owner_source = normalize_str(row.get("OwnerSource"))
        app_name_source = normalize_str(row.get("app_name_source"))

        billing_id = normalize_str(row.get("billing_owner_appsvcid"))
        support_id = normalize_str(row.get("support_owner_appsvcid"))

        app_name = normalize_str(row.get("application_name"))
        owner_name = normalize_str(row.get("billing_owner_name"))

        has_valid_appsvc = final_id.startswith(("app", "bsn"))
        has_person_or_app = bool(app_name or owner_name)

        # ----- ownership method -----
        if not has_valid_appsvc:
            method = "unmapped"
        else:
            if owner_source == "resourcetag":
                method = "APM via Resource Owner Tag"
            elif owner_source == "rginherited":
                method = "APM via RG Tag Inference"
            elif billing_id.startswith(("app", "bsn")) or support_id.startswith(("app", "bsn")):
                method = "APM via Resource Tag ID"
            elif "name" in app_name_source or "email" in app_name_source:
                # covers cases like numeric ids (15797, 18034) not in invalid_ids
                method = "APM via Naming Pattern"
            else:
                method = "APM via Owner AppSvcID"

        # ----- confidence -----
        method_norm = normalize_str(method)
        if "naming pattern" in method_norm:
            confidence = 60
        elif "rg tag inference" in method_norm:
            confidence = 80
        elif "resource owner tag" in method_norm or "resource tag id" in method_norm:
            confidence = 100
        else:
            confidence = 0

        # ----- final orphan flag + reason -----
        if orig_orphan == 0:
            final_orphan = 0
            orphan_reason = None
        else:
            # still original orphaned resource; see if tags resolved it
            if has_valid_appsvc and has_person_or_app:
                final_orphan = 0
                orphan_reason = "resolved_via_tags"
            else:
                final_orphan = 1
                if not final_id or final_id in invalid_ids:
                    orphan_reason = "missing_tags"
                else:
                    orphan_reason = "no_tags"

        # ----- hash key for change detection -----
        parts = [
            str(row.get("ResourceId") or ""),
            str(final_id_raw or ""),
            str(row.get("application_name") or ""),
            str(row.get("billing_owner_email") or ""),
        ]
        hash_key = hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()

        return pd.Series(
            [
                method,
                confidence,
                final_orphan,
                orphan_reason,
                hash_key,
            ],
            index=[
                "ownership_determination_method",
                "ownership_confidence_score",
                "is_orphaned",
                "orphan_reason",
                "HashKey",
            ],
        )

    ownership_fields = final_df.apply(compute_ownership_fields, axis=1)
    final_df[ownership_fields.columns] = ownership_fields

    # -------------- null out fields when final id is invalid --------------

    invalid_final_id_mask = final_df["final_app_service_id"].astype(str).str.lower().str.strip().isin(invalid_ids)

    for c in cols_to_null_when_invalid:
        if c in final_df.columns:
            final_df.loc[invalid_final_id_mask, c] = None

    # -------------- timestamps / metadata --------------

    now_utc = pd.Timestamp.utcnow()
    if "FirstSeenDate" not in final_df.columns:
        final_df["FirstSeenDate"] = now_utc

    final_df["LastVerifiedDate"] = now_utc
    if "LastModifiedDate" not in final_df.columns:
        final_df["LastModifiedDate"] = None
    final_df["ProcessingDate"] = now_utc

    return final_df


# --- FIX: ensure AppID is populated (fallback → final_app_service_id) ---

def pick_owner_appid(row: pd.Series):
    # 1) prefer AppID from SNOW
    appid = row.get("AppID")
    if isinstance(appid, str) and appid.strip():
        return appid.strip()

    # 2) then tag app_id
    tag_appid = row.get("app_id")
    if isinstance(tag_appid, str) and tag_appid.strip():
        return tag_appid.strip()

    # 3) fallback → final_app_service_id (only if valid)
    fa = row.get("final_app_service_id")
    if isinstance(fa, str):
        v = fa.strip()
        v_norm = normalize_str(v)
        if (
            v_norm
            and v_norm not in invalid_ids
            and (v_norm.startswith("app") or v_norm.startswith("bsn") or v_norm.isdigit())
        ):
            return v

    return None

# APPLY FIX FOR BOTH BILLING & SUPPORT OWNER APPID
final_df["billing_owner_appid"] = final_df.apply(pick_owner_appid, axis=1)
final_df["support_owner_appid"] = final_df["billing_owner_appid"]













# ======================================================
#      EAPM FALLBACK (from SnowNormalizedStaging)
# ======================================================

# Build EAPM lookup from apps_df (Snow)
eapm = apps_df[["EapmId", "AppOwner", "AppOwnerEmail"]].copy()
eapm = eapm.dropna(subset=["EapmId"])

# normalize keys
eapm["eapm_key"] = eapm["EapmId"].astype(str).str.strip().str.lower()

# build name/email lookup
eapm_name_lookup = (
    eapm.drop_duplicates("eapm_key").set_index("eapm_key")["AppOwner"]
)
eapm_email_lookup = (
    eapm.drop_duplicates("eapm_key").set_index("eapm_key")["AppOwnerEmail"]
)

# helper to normalize ids
def norm_id(val):
    if pd.isna(val):
        return None
    return str(val).strip().lower()

# normalize appsvcid columns for lookup
final_df["billing_eapm_key"] = final_df["billing_owner_appsvcid"].apply(norm_id)
final_df["support_eapm_key"]  = final_df["support_owner_appsvcid"].apply(norm_id)

# fill BILLING owner name from EAPM
final_df["billing_owner_name"] = final_df["billing_owner_name"].where(
    final_df["billing_owner_name"].notna(),
    final_df["billing_eapm_key"].map(eapm_name_lookup)
)

# fill BILLING owner email from EAPM
final_df["billing_owner_email"] = final_df["billing_owner_email"].where(
    final_df["billing_owner_email"].notna(),
    final_df["billing_eapm_key"].map(eapm_email_lookup)
)

# fill SUPPORT owner name from EAPM
final_df["support_owner_name"] = final_df["support_owner_name"].where(
    final_df["support_owner_name"].notna(),
    final_df["support_eapm_key"].map(eapm_name_lookup)
)

# fill SUPPORT owner email from EAPM
final_df["support_owner_email"] = final_df["support_owner_email"].where(
    final_df["support_owner_email"].notna(),
    final_df["support_eapm_key"].map(eapm_email_lookup)
)
 





# after invalid_ids = {...} and after you have apps_df / snow_df loaded
valid_eapm_ids = set(
    apps_df.get("EapmId")  # or SnowNormalizedStaging DF – whichever you’re using
        .dropna()
        .astype(str)
        .str.strip()
        .str.lower()
        .unique()
)






    # ---------- orphan reason ----------
    orig_orphan = int(row.get("original_is_orphaned") or 0)

    final_id_raw = row.get("final_app_service_id")
    final_id = normalize_str(final_id_raw)

    app_name = normalize_str(row.get("application_name"))
    owner_name = normalize_str(row.get("billing_owner_name"))
    has_valid_appsvc = final_id.startswith(("app", "bsn"))
    has_person_or_app = bool(app_name or owner_name)

    if orig_orphan == 0:
        final_orphan = 0
        orphan_reason = None
    else:
        if has_valid_appsvc and has_person_or_app:
            final_orphan = 0
            orphan_reason = "resolved_via_tags"
        else:
            # still orphaned
            final_orphan = 1

            # 1) SPECIAL CASE: EAPM-style numeric ID that is NOT in Snow
            # here we look at the appid columns which are storing EAPM IDs (18034 etc)
            eapm_val = normalize_str(
                row.get("billing_owner_appid") or row.get("support_owner_appid")
            )
            if eapm_val and eapm_val not in invalid_ids and eapm_val not in valid_eapm_ids:
                orphan_reason = "invalid_eapm_id"

            # 2) Normal cases
            elif not final_id or final_id in invalid_ids:
                orphan_reason = "missing_tags"
            else:
                orphan_reason = "no_tags"




# ---------- confidence ----------
method_norm = normalize_str(method)
final_id_raw = normalize_str(row.get("final_app_service_id") or row.get("final_app_service_id_raw") or "")

if "naming pattern" in method_norm:
    # EAPM-style numeric IDs (18034, 17535, etc.) → 60
    if final_id_raw.isdigit():
        confidence = 60
    # Emails or plain names in the "ID" field → 30
    else:
        confidence = 30

elif "rg tag inference" in method_norm:
    confidence = 80
elif "resource owner tag" in method_norm or "resource tag id" in method_norm:
    confidence = 100
else:
    confidence = 0



# -------------------------------------------------------
# EAPM OVERRIDE BLOCK (add here before return final_df)
# -------------------------------------------------------

# Build normalized EAPM lookup from Snow
eapm_map = apps_df[["EapmId", "AppOwner", "AppOwnerEmail",
                    "BusinessUnit", "Department"]].copy()

eapm_map["EapmId_norm"] = (
    eapm_map["EapmId"].astype(str).str.strip().str.lower()
)

# Normalize final ID for joining
final_df["final_id_norm"] = (
    final_df["final_app_service_id"].astype(str).str.strip().str.lower()
)

# Join Snow metadata
final_df = final_df.merge(
    eapm_map,
    left_on="final_id_norm",
    right_on="EapmId_norm",
    how="left",
    suffixes=("", "_eapm")
)

# Override owner fields ONLY for orphaned rows mapped by EAPM
mask_eapm_orphan = (
    (final_df.get("is_orphaned", 0) == 1)
    & final_df["AppOwner_eapm"].notna()
)

final_df.loc[mask_eapm_orphan, "billing_owner_name"]  = final_df["AppOwner_eapm"]
final_df.loc[mask_eapm_orphan, "support_owner_name"]  = final_df["AppOwner_eapm"]
final_df.loc[mask_eapm_orphan, "billing_owner_email"] = final_df["AppOwnerEmail_eapm"]
final_df.loc[mask_eapm_orphan, "support_owner_email"] = final_df["AppOwnerEmail_eapm"]
final_df.loc[mask_eapm_orphan, "business_unit"]       = final_df["BusinessUnit_eapm"]
final_df.loc[mask_eapm_orphan, "department"]          = final_df["Department_eapm"]











    # ------------------------------------------------------------
    # EAPM enrichment: use numeric final_app_service_id (EapmId)
    # to pull owner / email / BU / department from Snow
    # ------------------------------------------------------------

    # Build EAPM lookup from apps_df (SnowNormalizedStaging)
    eapm_map = apps_df[["EapmId", "AppOwner", "AppOwnerEmail",
                        "BusinessUnit", "Department"]].copy()
    eapm_map["eapm_key"] = eapm_map["EapmId"].astype(str).str.strip().str.lower()
    eapm_map = eapm_map.drop_duplicates(subset="eapm_key")

    eapm_name_lookup  = eapm_map.set_index("eapm_key")["AppOwner"]
    eapm_email_lookup = eapm_map.set_index("eapm_key")["AppOwnerEmail"]
    eapm_bu_lookup    = eapm_map.set_index("eapm_key")["BusinessUnit"]
    eapm_dept_lookup  = eapm_map.set_index("eapm_key")["Department"]

    def norm_id(val):
        if pd.isna(val):
            return None
        s = str(val).strip().lower()
        # treat only pure-digit ids as EAPM ids (18034, 17535,…)
        return s if s.isdigit() else None

    # key based on the final app id we chose (can be EapmId)
    final_df["eapm_key"] = final_df["final_app_service_id"].apply(norm_id)
    mask_eapm = final_df["eapm_key"].notna()

    # Fill NAME from EAPM (only where we have an EAPM match)
    final_df.loc[mask_eapm, "billing_owner_name"] = (
        final_df.loc[mask_eapm, "billing_owner_name"]
        .combine_first(final_df.loc[mask_eapm, "eapm_key"].map(eapm_name_lookup))
    )
    final_df.loc[mask_eapm, "support_owner_name"] = (
        final_df.loc[mask_eapm, "support_owner_name"]
        .combine_first(final_df.loc[mask_eapm, "eapm_key"].map(eapm_name_lookup))
    )

    # Fill EMAIL from EAPM
    final_df.loc[mask_eapm, "billing_owner_email"] = (
        final_df.loc[mask_eapm, "billing_owner_email"]
        .combine_first(final_df.loc[mask_eapm, "eapm_key"].map(eapm_email_lookup))
    )
    final_df.loc[mask_eapm, "support_owner_email"] = (
        final_df.loc[mask_eapm, "support_owner_email"]
        .combine_first(final_df.loc[mask_eapm, "eapm_key"].map(eapm_email_lookup))
    )

    # Fill BU + Department from EAPM
    final_df.loc[mask_eapm, "business_unit"] = (
        final_df.loc[mask_eapm, "business_unit"]
        .combine_first(final_df.loc[mask_eapm, "eapm_key"].map(eapm_bu_lookup))
    )
    final_df.loc[mask_eapm, "department"] = (
        final_df.loc[mask_eapm, "department"]
        .combine_first(final_df.loc[mask_eapm, "eapm_key"].map(eapm_dept_lookup))
    )
