# azure_inactive_resource_table.py
#
# One-time inactive-resources backfill:
# 1) Read inactive resource + inferred tags + SNOW app tables from Lenticular
# 2) Run the same Pandas transform logic you already have
# 3) Load the result into Gold inactive table (or staging table) in HybridASA

import os
import uuid
import hashlib
from typing import Optional

import pandas as pd
import pyodbc
from concurrent.futures import ThreadPoolExecutor, as_completed


# =============================================================================
# 1. CONNECTION HELPERS
# =============================================================================

# --- Fill these from your keyvault / env variables or hard-code for local test
LENTICULAR_SERVER = os.getenv("LENTICULAR_SERVER", r"<lenticular-sql-server>")
LENTICULAR_DB     = os.getenv("LENTICULAR_DB",     "Lenticular")
LENTICULAR_USER   = os.getenv("LENTICULAR_USER",   "<lenticular-username>")
LENTICULAR_PW     = os.getenv("LENTICULAR_PW",     "<lenticular-password>")

HYBRIDASA_SERVER  = os.getenv("HYBRIDASA_SERVER",  r"<hybridasa-sql-server>")
HYBRIDASA_DB      = os.getenv("HYBRIDASA_DB",      "HybridASA")
HYBRIDASA_USER    = os.getenv("HYBRIDASA_USER",    "<hybridasa-username>")
HYBRIDASA_PW      = os.getenv("HYBRIDASA_PW",      "<hybridasa-password>")

# Target Gold table (inactive) – change _stg to final if you want to load direct
TARGET_TABLE = "[Gold].[InactiveAzureGoldResourceNormalized_stg]"


def connect(server: str, database: str, username: str, password: str) -> pyodbc.Connection:
    """
    Create a basic pyodbc connection using SQL auth.
    """
    conn_str = (
        "DRIVER={{ODBC Driver 17 for SQL Server}};"
        "SERVER={server};DATABASE={db};UID={user};PWD={pw};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    ).format(server=server, db=database, user=username, pw=password)

    return pyodbc.connect(conn_str)


def read_sql_df(conn: pyodbc.Connection, sql: str, chunksize: int = 500_000) -> pd.DataFrame:
    """
    Read a large query in chunks and return a single DataFrame.
    """
    chunks = []
    for chunk in pd.read_sql(sql, conn, chunksize=chunksize):
        chunks.append(chunk)
    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True)


# =============================================================================
# 2. LOAD SOURCES (INACTIVE RESOURCES + INFERRED TAGS + SNOW APPS)
# =============================================================================

def load_sources() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Loads:
      - inactive resources from [silver].[AzureInActiveResourcesNormalized]
      - inferred tags from [Gold].[vrag_Azure_InferredTags]
      - app metadata from [Silver].[SnowNormalized]
    """
    # --- Lenticular connection for all reads
    with connect(LENTICULAR_SERVER, LENTICULAR_DB, LENTICULAR_USER, LENTICULAR_PW) as con_l:

        # INACTIVE resources table (7M+ rows)
        resources_sql = """
        SELECT
            [ResourceId],
            [ResourceName],
            [ResourceType],
            [CloudProvider],
            [CloudAccountId],
            [AccountName],
            [ResourceGroupName],
            [Region],
            [Environment],
            [ResHumanaResourceID],
            [ResHumanaConsumerID],
            [RgHumanaResourceID],
            [RgHumanaConsumerID],
            [EffectiveHumanaResourceID],
            [EffectiveHumanaConsumerID],
            [BillingOwnerAppsvcid],
            [SupportOwnerAppsvcid],
            [OwnerSource],
            [isPlatformManaged],
            [ManagementModel],
            [isOrphaned],
            [isDeleted],
            [hasConflictingTags],
            [DependencyTriggeredUpdate],
            [HashKey],
            [ChangeCategory],
            [FirstSeenDate],
            [LastVerifiedDate],
            [LastModifiedDate],
            [ProcessingDate],
            [AccountEnv],
            [CloudVersion],
            [ResTags]
        FROM [silver].[AzureInActiveResourcesNormalized];
        """

        resources_df = read_sql_df(con_l, resources_sql, chunksize=500_000)

        # Inferred tags (vrag Azure inferred tags view/table)
        inferred_sql = """
        SELECT
            [resource_id],
            [resource_name],
            [resource_group_name],
            [subscription_id],
            [resource_type],
            [cloud_provider],
            [subscription_name],
            [environment],
            [change_category],
            [data_hash],
            [inferred_app_name],
            [ml_app_name],
            [app_owner],
            [app_owner_email],
            [app_pattern],
            [app_name_source],
            [confidence_score],
            [rg_inferred_app_name],
            [res_tags],
            [processing_date]
        FROM [Gold].[vrag_Azure_InferredTags];
        """

        inferred_df = read_sql_df(con_l, inferred_sql, chunksize=200_000)

        # SNOW normalized apps (for EAPM owner / email)
        snow_sql = """
        SELECT
            COALESCE([AppServiceID], [_APPID]) AS [AppServiceID],
            [AppName],
            [AppOwnerEmail],
            [AppOwner],
            [BusinessUnit],
            [Department]
        FROM [Silver].[SnowNormalized];
        """

        snow_df = read_sql_df(con_l, snow_sql, chunksize=100_000)

    print(f"Loaded inactive resources: {len(resources_df):,} rows")
    print(f"Loaded inferred tags:      {len(inferred_df):,} rows")
    print(f"Loaded SNOW apps:          {len(snow_df):,} rows")

    return resources_df, inferred_df, snow_df


# =============================================================================
# 3. TRANSFORMATION LOGIC  (COPY OF YOUR EXISTING GOLD TRANSFORM)
# =============================================================================

# NOTE:
# Below is structured to match what you already have in your
# ETL_FinalGoldResourceOwnerMapping_1.ipynb.
# I am re-creating the main pieces based on your screenshots.
# If you see any mismatch, you can copy/paste directly from your
# existing notebook into this transform() function body.

def normalize_str(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s.lower() if s else None


def looks_like_email(val: Optional[str]) -> bool:
    if not val:
        return False
    s = str(val)
    return "@" in s and "." in s


def categorize_resource_type(rt: str) -> str:
    """
    Your resource type standardization mapping.
    For now just return the incoming value;
    plug in your existing categorize logic here if needed.
    """
    return rt


def transform(resource_df: pd.DataFrame,
              virtual_tags_df: pd.DataFrame,
              snow_df: pd.DataFrame) -> pd.DataFrame:
    """
    Core inactive-gold transformation.

    This is a direct port of your existing `transform()` logic:
    - normalize keys
    - join inferred tags
    - orphan vs non-orphan logic
    - choose final AppServiceID + ownership source
    - compute hash_key and metadata columns
    - rename columns into Gold schema
    """

    # ---------------------------------------------------------------------
    # A. normalize EAPM IDs (from SNOW) and build lookups
    # ---------------------------------------------------------------------
    valid_eapm_ids = set(
        snow_df.get("EapmId")
              .dropna()
              .astype(str)
              .str.strip()
              .str.lower()
              .unique()
    )

    # Normalize resource_id key for joins
    resource_df["resource_id_key"] = resource_df["ResourceId"].astype(str).str.lower()
    virtual_tags_df["resource_id_key"] = virtual_tags_df["resource_id"].astype(str).str.lower()

    # ---------------------------------------------------------------------
    # B. Orphan mask split
    # ---------------------------------------------------------------------
    orphan_mask = resource_df["isOrphaned"].fillna(0).astype(int).eq(1)
    orphaned_res_df = resource_df[orphan_mask].copy()
    non_orphaned_res_df = resource_df[~orphan_mask].copy()

    # ---------------------------------------------------------------------
    # C. Merge inferred tags into non-orphaned and orphaned
    # ---------------------------------------------------------------------
    # non-orphaned
    non_orphaned_res_df["primary_appservice"] = non_orphaned_res_df["BillingOwnerAppsvcid"].astype(str)
    virtual_tags_sub = virtual_tags_df[[
        "resource_id_key",
        "app_service_id",
        "inferred_app_name",
        "ml_app_name",
        "app_owner",
        "app_owner_email",
        "app_pattern",
        "app_name_source",
        "confidence_score",
        "rg_inferred_app_name",
        "res_tags",
    ]].copy()

    non_orphaned_with_tags = non_orphaned_res_df.merge(
        virtual_tags_sub,
        on="resource_id_key",
        how="left",
        suffixes=("", "_tags")
    )

    # orphaned
    orphaned_res_df["primary_appservice"] = orphaned_res_df["BillingOwnerAppsvcid"].astype(str)

    orphan_tags = orphaned_res_df.merge(
        virtual_tags_sub,
        on="resource_id_key",
        how="left",
        suffixes=("", "_tags")
    )

    # ---------------------------------------------------------------------
    # D. Helper funcs used below (APP service id pick and source)
    # ---------------------------------------------------------------------
    def pick_app_service_id(row: pd.Series) -> Optional[str]:
        """
        Your three-level APP ID selection:
        1) primary_appservice (4.1)
        2) app_service_id from tags
        3) fall back to app_id from tags (if you used that logic)
        """
        primary = normalize_str(row.get("primary_appservice"))
        tag_appsvc = normalize_str(row.get("app_service_id"))
        tag_appid = normalize_str(row.get("app_id"))  # only if exists

        # 1. primary from 4.1
        if primary and primary not in invalid_ids:
            return primary.strip()

        # 2. app_service_id from tags
        if tag_appsvc and tag_appsvc not in invalid_ids:
            return tag_appsvc.strip()

        # 3. fall back to app_id when app_service_id is null
        if tag_appid and tag_appid not in invalid_ids:
            return tag_appid.strip()

        return None

    def infer_appsvc_source_non_orphan(row: pd.Series) -> str:
        """
        Decide whether final app service id came from resource table vs tags table.
        """
        primary = normalize_str(row.get("primary_appservice"))
        tag_val = normalize_str(row.get("app_service_id"))
        fa = normalize_str(row.get("final_app_service_id"))

        if fa and primary and fa == primary:
            return "resource table"
        if fa and tag_val and fa == tag_val:
            return "tags table"
        return "None"

    # invalid_ids set from your earlier code
    invalid_ids = {"0", "na", "nan", "none", ""}

    # final_app_service_id for non-orphaned
    non_orphaned_with_tags["final_app_service_id"] = \
        non_orphaned_with_tags.apply(pick_app_service_id, axis=1)

    non_orphaned_with_tags["appsvc_source"] = \
        non_orphaned_with_tags.apply(infer_appsvc_source_non_orphan, axis=1)

    # normalize final_app_service_id for join to SNOW later
    non_orphaned_with_tags["appserviceid_key"] = \
        non_orphaned_with_tags["final_app_service_id"].astype(str).str.lower()

    # ---------------------------------------------------------------------
    # E. Merge SNOW app owner / email / BU / Dept
    # ---------------------------------------------------------------------
    snow_appsvc = snow_df[["AppServiceID", "AppName", "AppOwnerEmail",
                           "AppOwner", "BusinessUnit", "Department"]].copy()
    snow_appsvc["appserviceid_key"] = snow_appsvc["AppServiceID"].astype(str).str.lower()

    # Build simple lookups for EAPM
    eapm_map = snow_df[["EapmId", "AppOwner", "AppOwnerEmail",
                        "Businessunit", "Department"]].copy()
    eapm_map["eapm_key"] = eapm_map["EapmId"].astype(str).str.strip().str.lower()
    eapm_map = eapm_map.drop_duplicates(subset=["eapm_key"])

    eapm_name_lookup = eapm_map.set_index("eapm_key")["AppOwner"].to_dict()
    eapm_email_lookup = eapm_map.set_index("eapm_key")["AppOwnerEmail"].to_dict()
    eapm_bu_lookup = eapm_map.set_index("eapm_key")["Businessunit"].to_dict()
    eapm_dept_lookup = eapm_map.set_index("eapm_key")["Department"].to_dict()

    # join SNOW app svc
    non_orphaned_join = non_orphaned_with_tags.merge(
        snow_appsvc,
        on="appserviceid_key",
        how="left",
        suffixes=("", "_snow")
    )

    # ---------------------------------------------------------------------
    # F. Ownership / owner name / email logic (same as your notebook)
    # ---------------------------------------------------------------------
    final_df = non_orphaned_join.copy()

    # Helper to choose owner name from multiple columns
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

    # final owner name / email
    final_df["billing_owner_name"] = final_df.apply(pick_owner_name, axis=1)
    final_df["support_owner_name"] = final_df["billing_owner_name"]

    final_df["billing_owner_email"] = final_df.apply(pick_owner_email, axis=1)
    final_df["support_owner_email"] = final_df["billing_owner_email"]

    # Business unit + department from EAPM (based on chosen app id)
    # build eapm_key here from final_app_service_id when it is numeric EAPM
    final_df["eapm_key"] = final_df["final_app_service_id"].astype(str).str.strip().str.lower()
    mask_eapm = final_df["eapm_key"].notna() & final_df["eapm_key"].isin(eapm_name_lookup.keys())

    final_df.loc[mask_eapm, "billing_owner_name"] = \
        final_df.loc[mask_eapm, "eapm_key"].map(eapm_name_lookup) \
            .combine_first(final_df.loc[mask_eapm, "billing_owner_name"])

    final_df.loc[mask_eapm, "support_owner_name"] = \
        final_df.loc[mask_eapm, "eapm_key"].map(eapm_name_lookup) \
            .combine_first(final_df.loc[mask_eapm, "support_owner_name"])

    final_df.loc[mask_eapm, "billing_owner_email"] = \
        final_df.loc[mask_eapm, "eapm_key"].map(eapm_email_lookup) \
            .combine_first(final_df.loc[mask_eapm, "billing_owner_email"])

    final_df.loc[mask_eapm, "support_owner_email"] = \
        final_df.loc[mask_eapm, "eapm_key"].map(eapm_email_lookup) \
            .combine_first(final_df.loc[mask_eapm, "support_owner_email"])

    final_df.loc[mask_eapm, "business_unit"] = \
        final_df.loc[mask_eapm, "eapm_key"].map(eapm_bu_lookup)

    final_df.loc[mask_eapm, "department"] = \
        final_df.loc[mask_eapm, "eapm_key"].map(eapm_dept_lookup)

    # ---------------------------------------------------------------------
    # G. Ownership method / confidence / orphan flags (your big case logic)
    # ---------------------------------------------------------------------
    def compute_ownership_fields(row: pd.Series) -> pd.Series:
        method = None
        confidence = 0
        orphan_reason = None

        orig_orphan = int(row.get("isOrphaned", 0) or 0)
        final_id_raw = row.get("final_app_service_id")
        final_id_norm = normalize_str(final_id_raw)

        billing_id = normalize_str(row.get("billing_owner_appsvcid"))
        support_id = normalize_str(row.get("support_owner_appsvcid"))

        final_orphan = orig_orphan

        # CASE 1: direct EAPMID in SNOW
        if final_id_norm and final_id_norm.isdigit() and final_id_norm in valid_eapm_ids:
            method = "Resource Tags EAPMID"
            confidence = 100
            final_orphan = 0
            orphan_reason = None

        # CASE 3/BSN / APP pattern etc – simplified copy
        elif (billing_id and billing_id.startswith(("app", "bsn"))) or \
             (support_id and support_id.startswith(("app", "bsn"))):
            method = "Virtual Tagging Resource Tag"
            confidence = 60
            orphan_reason = None

        # CASE 2: looks like email / naming pattern but not in SNOW
        elif final_id_norm and looks_like_email(final_id_norm):
            method = "Virtual Tagging Naming Pattern"
            confidence = 40
            orphan_reason = None

        # CASE 4: Unmapped
        else:
            method = None
            confidence = 0
            orphan_reason = "NoTag"

        # hash key over important ownership columns
        parts = [
            str(row.get("ResourceId") or ""),
            str(row.get("Region") or ""),
            str(row.get("Environment") or ""),
            str(row.get("Application_name") or ""),
            str(row.get("billing_owner_email") or ""),
            str(row.get("billing_owner_appsvcid") or ""),
            str(row.get("billing_owner_name") or ""),
            str(row.get("support_owner_email") or ""),
            str(row.get("support_owner_appsvcid") or ""),
            str(row.get("support_owner_name") or ""),
            str(method or ""),
            str(confidence or 0),
            str(final_orphan or 0),
        ]
        hash_key = hashlib.sha256("~".join(parts).encode("utf-8")).hexdigest()

        return pd.Series(
            {
                "ownership_determination_method": method,
                "ownership_confidence_score": confidence,
                "is_orphaned": final_orphan,
                "orphan_reason": orphan_reason,
                "hash_key": hash_key,
            }
        )

    ownership_fields = final_df.apply(compute_ownership_fields, axis=1)
    final_df[ownership_fields.columns] = ownership_fields

    # ---------------------------------------------------------------------
    # H. Add timestamps / meta and rename columns for Gold schema
    # ---------------------------------------------------------------------
    now_utc = pd.Timestamp.utcnow()

    final_df["mapping_created_date"] = now_utc
    final_df["is_current"] = True
    final_df["last_verified_date"] = final_df.get("LastVerifiedDate").fillna(now_utc)
    final_df["last_modified_date"] = final_df.get("LastModifiedDate").fillna(now_utc)

    # mapping_id + fact_guid
    final_df["fact_guid"] = [str(uuid.uuid4()) for _ in range(len(final_df))]
    final_df["mapping_guid"] = [str(uuid.uuid4()) for _ in range(len(final_df))]

    # Standardize resource type if you need
    if "ResourceType" in final_df.columns:
        final_df["resource_type_standardized"] = \
            final_df["ResourceType"].apply(categorize_resource_type)

    rename_map = {
        "ResourceId": "resource_id",
        "ResourceName": "resource_name",
        "ResourceType": "resource_type",
        "CloudProvider": "cloud_provider",
        "CloudAccountId": "cloud_account_id",
        "AccountName": "account_name",
        "ResourceGroupName": "resource_group_name",
        "Region": "region",
        "Environment": "environment",
        "ResHumanaResourceID": "resource_humana_resource_id",
        "ResHumanaConsumerID": "resource_humana_consumer_id",
        "RgHumanaResourceID": "rg_humana_resource_id",
        "RgHumanaConsumerID": "rg_humana_consumer_id",
        "EffectiveHumanaResourceID": "effective_humana_resource_id",
        "EffectiveHumanaConsumerID": "effective_humana_consumer_id",
        "BillingOwnerAppsvcid": "billing_owner_appsvcid",
        "SupportOwnerAppsvcid": "support_owner_appsvcid",
        "OwnerSource": "owner_source",
        "isPlatformManaged": "is_platform_managed",
        "ManagementModel": "management_model",
        "hasConflictingTags": "has_conflicting_tags",
        "DependencyTriggeredUpdate": "dependency_triggered_update",
        "ChangeCategory": "change_category",
        "FirstSeenDate": "first_seen_date",
        "ProcessingDate": "processing_date",
        "AccountEnv": "account_env",
        "CloudVersion": "cloud_version",
        "ResTags": "res_tags",
    }

    final_df.rename(columns=rename_map, inplace=True)

    # Final column ordering – adjust to match your Gold inactive table exactly
    final_columns = [
        "resource_id",
        "resource_name",
        "resource_type",
        "resource_type_standardized",
        "cloud_provider",
        "cloud_account_id",
        "account_name",
        "resource_group_name",
        "region",
        "environment",
        "resource_humana_resource_id",
        "resource_humana_consumer_id",
        "rg_humana_resource_id",
        "rg_humana_consumer_id",
        "effective_humana_resource_id",
        "effective_humana_consumer_id",
        "billing_owner_appsvcid",
        "support_owner_appsvcid",
        "billing_owner_name",
        "billing_owner_email",
        "support_owner_name",
        "support_owner_email",
        "owner_source",
        "is_platform_managed",
        "management_model",
        "is_orphaned",
        "is_deleted",
        "has_conflicting_tags",
        "dependency_triggered_update",
        "ownership_determination_method",
        "ownership_confidence_score",
        "orphan_reason",
        "hash_key",
        "business_unit",
        "department",
        "change_category",
        "first_seen_date",
        "last_verified_date",
        "last_modified_date",
        "processing_date",
        "res_tags",
        "mapping_created_date",
        "is_current",
        "fact_guid",
        "mapping_guid",
        "cloud_version",
        "account_env",
    ]

    # Only keep columns that exist
    final_df = final_df[[c for c in final_columns if c in final_df.columns]]

    return final_df


# =============================================================================
# 4. PARALLEL INSERT INTO GOLD INACTIVE TABLE
# =============================================================================

def insert_batch(batch: list[tuple], insert_sql: str) -> str:
    """
    Insert one batch of rows (list of tuples) using a fresh connection.
    """
    try:
        with connect(HYBRIDASA_SERVER, HYBRIDASA_DB, HYBRIDASA_USER, HYBRIDASA_PW) as con:
            cur = con.cursor()
            cur.fast_executemany = True
            cur.executemany(insert_sql, batch)
            con.commit()
        return f"Inserted {len(batch)} rows"
    except Exception as e:
        return f"Error inserting batch: {e}"


def insert_gold_parallel(gold_df: pd.DataFrame,
                         batch_size: int = 10_000,
                         max_workers: int = 4) -> None:
    """
    Parallel batch insert into Gold inactive table (TARGET_TABLE).
    """
    if gold_df.empty:
        print("Gold DataFrame is empty. Nothing to insert.")
        return

    # Convert NaN to None so pyodbc sends NULLs
    rows = gold_df.where(pd.notna(gold_df), None).values.tolist()

    # Build parametrized INSERT
    insert_sql = f"""
    INSERT INTO {TARGET_TABLE} (
        {', '.join(f'[{col}]' for col in gold_df.columns)}
    ) VALUES (
        {', '.join('?' for _ in gold_df.columns)}
    );
    """

    # Split into batches
    batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]
    print(f"Starting parallel insert of {len(rows):,} rows in {len(batches)} batches using {max_workers} workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(insert_batch, batch, insert_sql) for batch in batches]
        for future in as_completed(futures):
            print(future.result())

    print("All batches processed.")


# =============================================================================
# 5. MAIN DRIVER
# =============================================================================

def main():
    print("Step 1: Loading inactive resource, inferred tags, and SNOW sources...")
    resources_df, inferred_df, snow_df = load_sources()

    print("Step 2: Running Pandas transformation for inactive resources...")
    gold_df = transform(resources_df, inferred_df, snow_df)
    print(f"Transformation complete. Gold inactive DataFrame has {len(gold_df):,} rows.")

    print(f"Step 3: Inserting into {TARGET_TABLE} ...")
    insert_gold_parallel(gold_df, batch_size=10_000, max_workers=4)
    print("Inactive Gold load finished.")


if __name__ == "__main__":
    main()
