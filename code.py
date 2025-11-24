import uuid
import hashlib
from typing import Tuple, Optional

import pandas as pd
import pyodbc


# ================================================================
# 1. SQL helpers
# ================================================================

def read_sql_df(conn, sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, conn)


def connect(server: str, database: str, username: str, password: str):
    """
    Create a SQL Server connection using ODBC Driver 18.
    """
    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(connection_string)


# ================================================================
# 2. Load sources
#    - Active resources (4.1)     : [silver].[vw_ActiveResources]        (Lenticular)
#    - Inferred tags (VTags)      : [Gold].[VTag_Azure_InferredTags]     (Lenticular)
#    - Service catalog (Snow 4.2) : [Silver].[SnowNormalized]            (HybridASA)
# ================================================================

def load_sources(
    lenticular_server: str,
    lenticular_database: str,
    lenticular_username: str,
    lenticular_password: str,
    hybrid_server: str,
    hybrid_database: str,
    hybrid_username: str,
    hybrid_password: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    # ---------- Lenticular: active resources (4.1) ----------
    with connect(
        lenticular_server,
        lenticular_database,
        lenticular_username,
        lenticular_password,
    ) as con_l:

        resources_sql = """
        SELECT
            [ResourceId],
            [ResourceName],
            [ResourceGroupName],
            [ResourceType],
            [CloudProvider],
            [CloudAccountId],
            [AccountName],
            [Region],
            [Environment],
            [ResHumanaResourceID],
            [ResHumanaConsumerID],
            [RgHumanaResourceID],
            [RgHumanaConsumerID],
            [EffectiveHumanaResourceId],
            [EffectiveHumanaConsumerId],
            [BillingOwnerAppSvcid],
            [SupportOwnerAppSvcid],
            [OwnerSource],
            [isPlatformManaged],
            [ManagementModel],
            [isOrphaned],
            [isDeleted],
            [hasConflictingTags],
            [DependencyTriggeredUpdate],
            [TagQualityScore],
            [ResTags],
            [HashKey],
            [ChangeCategory],
            [FirstSeenDate],
            [LastVerifiedDate],
            [LastModifiedDate],
            [ProcessingDate],
            [AccountEnv],
            [CloudVersion],
            [RgTags],
            [AuditID]
        FROM [silver].[vw_ActiveResources];   -- already isDeleted = 0
        """

        resources_df = read_sql_df(con_l, resources_sql)

        # ---------- Lenticular: VTags / inferred tags ----------
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
            [res_tags],
            [data_hash],
            [inferred_app_name],
            [app_name_source],
            [confidence_score],
            [app_owner],
            [ml_app_name],
            [ml_app_owner],
            [app_pattern],
            [app_id],
            [app_id_source_tag],
            [app_service_id],
            [rg_inferred_app_name],
            [processing_date]
        FROM [Gold].[VTag_Azure_InferredTags];
        """

        inferred_df = read_sql_df(con_l, inferred_sql)

    # ---------- HybridASA: Snow app catalog (4.2) ----------
    with connect(
        hybrid_server,
        hybrid_database,
        hybrid_username,
        hybrid_password,
    ) as con_h:

        apps_sql = """
        SELECT
            COALESCE(AppServiceID, AppID) AS AppServiceID,   -- join key
            [AppID],
            [AppName],
            [EapmId],
            [AppOwnerEmail],
            [AppOwner],
            [BusinessUnit],
            [Department],
            [BusinessCriticality],
            [Environment] AS AppEnvironment,
            [AppActive],
            [HashKey],
            [AuditId],
            [ChangeCategory],
            [CreatedDate]
        FROM [Silver].[SnowNormalized];
        """

        apps_df = read_sql_df(con_h, apps_sql)

    return resources_df, inferred_df, apps_df


# ================================================================
# 3. Resource type categorization (reporting dimension)
# ================================================================

resource_type_map = {
    "microsoft.network/publicipaddresses": "Networking",
    "microsoft.network/dnszones": "Networking",
    "microsoft.network/loadbalancers": "Networking",
    "microsoft.network/expressroutegateways": "Networking",
    "microsoft.compute/virtualmachines": "Compute",
    "microsoft.compute/virtualmachinescalesets": "Compute",
    "microsoft.compute/disks": "Storage",
    "microsoft.sql/servers/databases": "Database",
    "microsoft.dbforpostgresql/servers": "Database",
    "microsoft.web/serverfarms": "App Service",
    "microsoft.web/sites": "App Service",
    "microsoft.synapse/workspaces": "Analytics",
    "microsoft.storage/storageaccounts": "Storage",
    "microsoft.keyvault/vaults": "Security",
    # default: "Other"
}


def categorize_resource_type(rt: str) -> Optional[str]:
    if not isinstance(rt, str):
        return None
    return resource_type_map.get(rt.lower(), "Other")


# ================================================================
# 4. Shared helpers
# ================================================================

def normalize_str(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


# values of AppSvc/AppId we NEVER trust
invalid_ids = {
    "unknown/orphaned",
    "bill to subscription owner",
    "needs update",
    "orphaned",
    "none",
}


def pick_app_service_id(row: pd.Series) -> Optional[str]:
    """
    Extracts AppServiceID from BillingOwnerAppSvcid or SupportOwnerAppSvcid
    (first one that starts with APP/BSN).
    """
    for col in ["BillingOwnerAppSvcid", "SupportOwnerAppSvcid"]:
        val = row.get(col)
        if isinstance(val, str):
            v = val.strip()
            if len(v) >= 3 and v[:3].lower() in ("app", "bsn"):
                return v
    return None


# ================================================================
# 5. Transform -> Gold 5.1
# ================================================================

def transform(
    resources_df: pd.DataFrame,
    inferred_df: pd.DataFrame,
    apps_df: pd.DataFrame,
) -> pd.DataFrame:

    # ---------------------------
    # Copy inputs
    # ---------------------------
    res = resources_df.copy()
    inf = inferred_df.copy()
    apps = apps_df.copy()

    # Normalized join keys
    res["resource_id_key"] = res["ResourceId"].astype(str).str.lower()
    inf["resource_id_key"] = inf["resource_id"].astype(str).str.lower()
    apps["appserviceid_key"] = apps["AppServiceID"].astype(str).str.lower()

    # Split orphaned / non-orphaned using 4.1 flag
    orphan_mask = res["isOrphaned"].fillna(0).astype(int).eq(1)
    orphaned_res = res[orphan_mask].copy()
    non_orphaned_res = res[~orphan_mask].copy()

    # ==================================================
    # A. NON-ORPHANED FLOW  (Snow first, VTags as helper)
    # ==================================================

    # A1. Take AppSvc from 4.1 (billing/support)
    non_orphaned_res["primary_appservice_4_1"] = non_orphaned_res.apply(
        pick_app_service_id, axis=1
    )

    # A2. Join inferred tags
    non_orphaned_with_tags = non_orphaned_res.merge(
        inf[
            [
                "resource_id_key",
                "app_service_id",
                "app_id",
                "inferred_app_name",
                "ml_app_name",
                "ml_app_owner",
                "app_owner",
                "app_pattern",
                "app_name_source",
                "confidence_score",
                "rg_inferred_app_name",
                "res_tags",
            ]
        ],
        on="resource_id_key",
        how="left",
        suffixes=("", "_tag"),
    )

    def pick_non_orphan_appsvc(row: pd.Series) -> Optional[str]:
        # prefer AppSvc from 4.1 if it exists
        primary = row.get("primary_appservice_4_1")
        if isinstance(primary, str) and primary.strip():
            return primary.strip()

        # otherwise from VTag table (if not invalid)
        tag_val = row.get("app_service_id")
        if isinstance(tag_val, str):
            v = tag_val.strip()
            if v and normalize_str(v) not in invalid_ids:
                return v
        return None

    non_orphaned_with_tags["final_app_service_id"] = non_orphaned_with_tags.apply(
        pick_non_orphan_appsvc, axis=1
    )
    non_orphaned_with_tags["appserviceid_key"] = (
        non_orphaned_with_tags["final_app_service_id"].astype(str).str.lower()
    )

    # A3. Join with Snow
    non_orphaned_join = non_orphaned_with_tags.merge(
        apps,
        on="appserviceid_key",
        how="left",
        suffixes=("", "_snow"),
    )

    non_orphaned_join["ownership_path"] = non_orphaned_join.apply(
        lambda r: (
            "non_orphaned_snow"
            if isinstance(r.get("primary_appservice_4_1"), str)
            and r["primary_appservice_4_1"].strip()
            else "non_orphaned_tags_snow"
        ),
        axis=1,
    )

    # ==================================================
    # B. ORPHANED FLOW  (tags → Snow, with AppID fallback)
    # ==================================================

    orphan_tags = orphaned_res.merge(
        inf[
            [
                "resource_id_key",
                "app_service_id",
                "app_id",
                "inferred_app_name",
                "ml_app_name",
                "ml_app_owner",
                "app_owner",
                "app_pattern",
                "app_name_source",
                "confidence_score",
                "rg_inferred_app_name",
                "res_tags",
            ]
        ],
        on="resource_id_key",
        how="left",
        suffixes=("", "_tag"),
    )

    def pick_orphan_appsvc(row: pd.Series) -> Optional[str]:
        # 1) try AppSvc from 4.1 (in case flag is wrong)
        primary = pick_app_service_id(row)
        if primary:
            return primary.strip()

        # 2) use tag app_service_id if present & not invalid
        tag_appsvc = row.get("app_service_id")
        if isinstance(tag_appsvc, str):
            v = tag_appsvc.strip()
            if v and normalize_str(v) not in invalid_ids:
                return v

        # 3) fallback to tag app_id if it looks like an APP id and not invalid
        tag_appid = row.get("app_id")
        if isinstance(tag_appid, str):
            v = tag_appid.strip()
            v_norm = normalize_str(v)
            if v_norm.startswith("app") and v_norm not in invalid_ids:
                return v

        return None

    orphan_tags["final_app_service_id"] = orphan_tags.apply(
        pick_orphan_appsvc, axis=1
    )
    orphan_tags["appserviceid_key"] = (
        orphan_tags["final_app_service_id"].astype(str).str.lower()
    )

    orphan_join = orphan_tags.merge(
        apps,
        on="appserviceid_key",
        how="left",
        suffixes=("", "_snow"),
    )

    orphan_join["ownership_path"] = "orphaned_tags_snow"

    # ==================================================
    # C. Combine non-orphaned + orphaned
    # ==================================================
    common_cols = list(
        set(non_orphaned_join.columns).intersection(orphan_join.columns)
    )
    final_df = pd.concat(
        [
            non_orphaned_join[common_cols],
            orphan_join[common_cols],
        ],
        ignore_index=True,
    )

    # ==================================================
    # D. Enrich with application & owner data
    # ==================================================

    # AppSvc IDs from final_app_service_id
    final_df["billing_owner_appsvcid"] = final_df["final_app_service_id"]
    final_df["support_owner_appsvcid"] = final_df["final_app_service_id"]

    # AppID: prefer Snow.AppID; if NULL then tags.app_id
    final_df["billing_owner_appid"] = final_df["AppID"].combine_first(
        final_df["app_id"]
    )
    final_df["support_owner_appid"] = final_df["billing_owner_appid"]

    # EAPM
    final_df["billing_owner_eapmid"] = final_df["EapmId"]
    final_df["support_owner_eapmid"] = final_df["EapmId"]

    # Application name: Snow → inferred_app_name → ml_app_name → rg_inferred
    def safe_col(df: pd.DataFrame, col: str) -> pd.Series:
        return df[col] if col in df.columns else pd.Series([None] * len(df))

    final_df["application_name"] = (
        safe_col(final_df, "AppName")
        .combine_first(safe_col(final_df, "inferred_app_name"))
        .combine_first(safe_col(final_df, "ml_app_name"))
        .combine_first(safe_col(final_df, "rg_inferred_app_name"))
    )

    # ---------------- Owner fields in ONE pass ----------------

    def compute_owner_fields(row: pd.Series) -> pd.Series:
        # Owner name from Snow or tags
        owner_name = None
        for col in ("AppOwner", "app_owner", "ml_app_owner"):
            v = row.get(col)
            if isinstance(v, str) and v.strip():
                owner_name = v.strip()
                break

        # Owner email (Snow)
        owner_email = None
        v = row.get("AppOwnerEmail")
        if pd.notna(v) and str(v).strip():
            owner_email = str(v).strip()

        # Platform team name (for platform-managed)
        platform_team = None
        val = str(row.get("isPlatformManaged", "")).strip().lower()
        # NOTE: depending on your data, 1/0 meaning may differ; adjust if needed
        if val in ("1", "true"):
            platform_team = owner_name

        return pd.Series(
            [
                owner_name,
                owner_name,
                owner_email,
                owner_email,
                platform_team,
            ],
            index=[
                "billing_owner_name",
                "support_owner_name",
                "billing_owner_email",
                "support_owner_email",
                "platform_team_name",
            ],
        )

    owner_fields = final_df.apply(compute_owner_fields, axis=1)
    final_df[owner_fields.columns] = owner_fields

    # Business unit / department direct copy
    final_df["business_unit"] = safe_col(final_df, "BusinessUnit")
    final_df["department"] = safe_col(final_df, "Department")

    # ==================================================
    # E. Ownership determination method, confidence, orphan flags
    # ==================================================

    # keep original flag for debugging
    final_df["original_is_orphaned"] = final_df["isOrphaned"].fillna(0).astype(int)

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

        has_valid_appsvc = bool(final_id) and final_id.startswith(("app", "bsn"))
        has_appid = bool(normalize_str(row.get("billing_owner_appid")))
        has_person_or_app = bool(app_name or owner_name)

        # ---------- ownership method ----------
        if not has_valid_appsvc:
            method = "unmapped"
        else:
            if owner_source == "resourcetag":
                method = "APM via Resource Owner Tag"
            elif owner_source == "rginherited":
                method = "APM via RG Tag Inference"
            elif billing_id.startswith(("app", "bsn")) or support_id.startswith(
                ("app", "bsn")
            ):
                method = "APM via Resource Tag ID"
            elif "name" in app_name_source or "email" in app_name_source:
                method = "APM via Naming Pattern"
            else:
                method = "APM via Owner AppSvcID"

        # ---------- confidence ----------
        method_norm = normalize_str(method)
        if "resource owner tag" in method_norm or "resource tag id" in method_norm:
            confidence = 100
        elif "rg tag inference" in method_norm:
            confidence = 80
        elif "naming pattern" in method_norm:
            confidence = 60
        else:
            confidence = 0

        # Boost to 100 if we have both AppSvc and AppId
        if has_valid_appsvc and has_appid:
            confidence = 100

        # ---------- final orphan flag & reason ----------
        if orig_orphan == 0:
            final_orphan = 0
            orphan_reason = None
        else:
            if has_valid_appsvc and has_person_or_app:
                final_orphan = 0
                orphan_reason = "resolved_via_tags"
            else:
                final_orphan = 1
                if not final_id or final_id in invalid_ids:
                    orphan_reason = "missing_tags"
                else:
                    orphan_reason = "no_tags"

        # ---------- hash ----------
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
            ],
            index=[
                "ownership_determination_method",
                "ownership_confidence_score",
                "is_orphaned",
                "orphan_reason",
            ],
        ), hash_key

    # Apply ownership fields
    results = final_df.apply(
        lambda r: compute_ownership_fields(r), axis=1, result_type="expand"
    )

    # results is a Series of (Series, hash_key), so split:
    ownership_matrix = results.apply(lambda x: x[0])
    hash_keys = results.apply(lambda x: x[1])

    final_df[ownership_matrix.columns] = ownership_matrix
    final_df["hash_key"] = hash_keys

    # ==================================================
    # F. Timestamps & audit
    # ==================================================
    now_utc = pd.Timestamp.utcnow()

    final_df["mapping_created_date"] = now_utc
    final_df["is_current"] = True
    final_df["resource_created_date"] = final_df.get("FirstSeenDate")
    final_df["last_verified_date"] = final_df.get("LastVerifiedDate").fillna(now_utc)
    final_df["last_modified_date"] = final_df.get("LastModifiedDate").fillna(now_utc)

    final_df["u_id"] = [str(uuid.uuid4()) for _ in range(len(final_df))]
    final_df["audit_id"] = [str(uuid.uuid4()) for _ in range(len(final_df))]

    # ==================================================
    # G. Resource type standardisation
    # ==================================================
    final_df["resource_type_standardized"] = final_df["ResourceType"].apply(
        categorize_resource_type
    )

    # ==================================================
    # H. Rename + select Gold 5.1 columns
    # ==================================================
    rename_map = {
        "ResourceId": "resource_id",
        "ResourceName": "resource_name",
        "CloudProvider": "cloud_provider",
        "CloudAccountId": "cloud_account_id",
        "AccountName": "cloud_account_name",
        "Region": "region",
        "Environment": "environment",
        "isPlatformManaged": "is_platform_managed",
        "ManagementModel": "management_model",
        "isDeleted": "is_deleted",
        "hasConflictingTags": "has_conflicting_tags",
        "DependencyTriggeredUpdate": "dependency_triggered_update",
        "TagQualityScore": "tag_quality_score",
        "ChangeCategory": "change_category",
        "FirstSeenDate": "resource_created_date",
    }

    final_df.rename(columns=rename_map, inplace=True)

    final_columns = [
        "u_id",
        "resource_id",
        "resource_name",
        "resource_type_standardized",
        "cloud_provider",
        "cloud_account_id",
        "cloud_account_name",
        "region",
        "environment",
        "billing_owner_appsvcid",
        "support_owner_appsvcid",
        "billing_owner_appid",
        "support_owner_appid",
        "billing_owner_eapmid",
        "support_owner_eapmid",
        "billing_owner_name",
        "support_owner_name",
        "billing_owner_email",
        "support_owner_email",
        "application_name",
        "business_unit",
        "department",
        "is_platform_managed",
        "management_model",
        "platform_team_name",
        "ownership_confidence_score",
        "ownership_determination_method",
        "is_orphaned",
        "orphan_reason",
        "is_deleted",
        "has_conflicting_tags",
        "dependency_triggered_update",
        "tag_quality_score",
        "hash_key",
        "audit_id",
        "change_category",
        "resource_created_date",
        "mapping_created_date",
        "last_verified_date",
        "last_modified_date",
        "is_current",
        "original_is_orphaned",
        "ownership_path",
    ]

    final_df = final_df[[c for c in final_columns if c in final_df.columns]]

    return final_df


# ================================================================
# 6. (Optional) Insert into SQL + CSV export
# ================================================================

def insert_gold_parallel(
    gold_df: pd.DataFrame,
    server: str,
    database: str,
    username: str,
    password: str,
    table: str,
    batch_size: int = 5000,
):
    """
    Simple fast_executemany insert (single-threaded).
    If you want ThreadPoolExecutor parallelism, you can wrap this later.
    """
    # Replace NaN with None
    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in gold_df.itertuples(index=False, name=None)
    ]

    insert_sql = f"""
    INSERT INTO {table} (
        {', '.join(f'[{col}]' for col in gold_df.columns)}
    )
    VALUES (
        {', '.join('?' for _ in gold_df.columns)}
    );
    """

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )

    with pyodbc.connect(conn_str, autocommit=False) as con:
        cur = con.cursor()
        cur.fast_executemany = True

        total = len(rows)
        print(f"Inserting {total} rows in batches of {batch_size}...")

        for start in range(0, total, batch_size):
            batch = rows[start:start + batch_size]
            cur.executemany(insert_sql, batch)

        con.commit()

    print("✅ Insert complete.")


# ================================================================
# 7. Example main (for testing)
# ================================================================
if __name__ == "__main__":
    # TODO: fill in your real connection details or use env vars
    lenticular_server = "<lenticular_server>"
    lenticular_database = "lenticular"
    lenticular_username = "<username>"
    lenticular_password = "<password>"

    hybrid_server = "<hybridasa_server>"
    hybrid_database = "hybridasa"
    hybrid_username = "<username>"
    hybrid_password = "<password>"

    print("Loading source data...")
    resources_df, inferred_df, apps_df = load_sources(
        lenticular_server,
        lenticular_database,
        lenticular_username,
        lenticular_password,
        hybrid_server,
        hybrid_database,
        hybrid_username,
        hybrid_password,
    )

    print("Shapes after load_sources()")
    print("resources_df:", resources_df.shape)
    print("inferred_df :", inferred_df.shape)
    print("apps_df     :", apps_df.shape)

    print("\nTransforming to Gold 5.1...")
    gold_df = transform(resources_df, inferred_df, apps_df)

    print("\nSchema and Data Types:")
    print(gold_df.dtypes)
    print("\nTotal Rows:", len(gold_df))
    print("\nSample Rows:")
    print(gold_df.head(20))

    # Save CSV for validation
    output_path = r"C:\temp\gold_5_1_output.csv"
    gold_df.to_csv(output_path, index=False)
    print(f"\nCSV exported to: {output_path}")

    # Example insert call (uncomment and set target table)
    # insert_gold_parallel(
    #     gold_df,
    #     server=hybrid_server,
    #     database=hybrid_database,
    #     username=hybrid_username,
    #     password=hybrid_password,
    #     table="[AZURE].[Final_Gold_Resources]",
    #     batch_size=5000,
    # )


    # ---------- ownership method ----------
    has_any_appsvc = bool(final_id_raw)          # any value at all
    has_valid_tag_id = final_id.startswith(("app", "bsn"))
    is_invalid_code = final_id in invalid_ids

    if owner_source == "resourcetag":
        method = "APM via Resource Owner Tag"

    elif owner_source == "rginherited":
        method = "APM via RG Tag Inference"

    # Proper tag-based ID (app*/bsn*), not invalid
    elif has_valid_tag_id and not is_invalid_code:
        method = "APM via Resource Tag ID"

    # Values like "15797": not invalid, not tag-based → naming pattern
    elif has_any_appsvc and not is_invalid_code:
        method = "APM via Naming Pattern"

    # Source indicates naming/email pattern
    elif "name" in app_name_source or "email" in app_name_source:
        method = "APM via Naming Pattern"

    # fallback: valid appsvc but couldn’t classify
    elif has_any_appsvc:
        method = "APM via Owner AppSvcID"

    else:
        method = "unmapped"

def normalize_str(value):
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def pick_orphaned_appsvc(row: pd.Series) -> Optional[str]:
    # 1) Prefer primary appsvc (from snow or merged logic)
    primary = pick_app_service_id(row)
    if primary:
        v_norm = normalize_str(primary)
        if v_norm and v_norm not in invalid_ids:
            return primary.strip()

    # 2) Use tag app_service_id if present and not invalid
    tag_appsvc = row.get("app_service_id")
    if isinstance(tag_appsvc, str):
        v = tag_appsvc.strip()
        if v and normalize_str(v) not in invalid_ids:
            return v

    # 3) NEW: fall back to app_id if no appsvcid and app_id is not invalid
    tag_appid = row.get("app_id")
    if isinstance(tag_appid, str):
        v = tag_appid.strip()
        if v and normalize_str(v) not in invalid_ids:
            return v

    # nothing usable
    return None
