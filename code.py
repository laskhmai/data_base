


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
# 4. Transform logic (orphan vs non-orphan + scoring)
# ================================================================

# Helper: App Service ID from 4.1 (billing/support)
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


def normalize_str(value) -> str:  # <<< NEW (helper for lower/strip)
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


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
    # A. NON-ORPHANED FLOW  (join Snow first)
    # ==================================================
    # A1. Take AppSvc from 4.1 (billing/support)
    non_orphaned_res["primary_appservice_4_1"] = non_orphaned_res.apply(
        pick_app_service_id, axis=1
    )

    # A2. Join inferred tags (for fallback app_service_id, names, etc.)
    non_orphaned_with_tags = non_orphaned_res.merge(
        inf[
            [
                "resource_id_key",
                "app_service_id",
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
        # otherwise from VTag table
        tag_val = row.get("app_service_id")
        if isinstance(tag_val, str) and tag_val.strip():
            return tag_val.strip()
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

    # Track path for debugging
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
    # B. ORPHANED FLOW  (tags → Snow, with 4.1 fallback)
    # ==================================================
    orphan_tags = orphaned_res.merge(
        inf[
            [
                "resource_id_key",
                "app_service_id",
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
        # try 4.1 AppSvc first (in case isOrphaned flag is wrong)
        primary = pick_app_service_id(row)
        if primary:
            return primary
        # else, use tags app_service_id
        tag_val = row.get("app_service_id")
        if isinstance(tag_val, str) and tag_val.strip():
            return tag_val.strip()
        return None

    orphan_tags["final_app_service_id"] = orphan_tags.apply(pick_orphan_appsvc, axis=1)
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
    # appsvc / appid / eapmid
    final_df["billing_owner_appsvcid"] = final_df["final_app_service_id"]
    final_df["support_owner_appsvcid"] = final_df["final_app_service_id"]
    final_df["billing_owner_appid"] = final_df["AppID"]
    final_df["support_owner_appid"] = final_df["AppID"]
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

    # Owner name / email
    def pick_owner_name(row: pd.Series) -> Optional[str]:
        for col in ["AppOwner", "app_owner", "ml_app_owner"]:
            v = row.get(col)
            if isinstance(v, str) and v.strip():
                return v.strip()
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

    # Business unit / department
    final_df["business_unit"] = safe_col(final_df, "BusinessUnit")
    final_df["department"] = safe_col(final_df, "Department")

    # Platform team (when platform managed)
    def platform_team(row: pd.Series) -> Optional[str]:
        val = str(row.get("isPlatformManaged", "")).strip().lower()
        if val in ("0", "true"):  # 0 = platform-managed in your data
            return str(row.get("support_owner_name") or "")
        return None

    final_df["platform_team_name"] = final_df.apply(platform_team, axis=1)

    # ==================================================
    # E. Ownership determination method & confidence
    # ==================================================

    invalid_ids = {  # <<< NEW – tokens we treat as "unmapped"
        "",
        "unknown/orphaned",
        "unknown / orphaned",
        "unknownorphaned",
        "bill to subscription owner",
        "needs update",
        "test",
        "none",
    }

    def determine_method(row: pd.Series) -> str:
        is_orphan = int(row.get("isOrphaned") or 0)
        final_id_raw = row.get("final_app_service_id")

        final_id = normalize_str(final_id_raw)
        owner_source = normalize_str(row.get("OwnerSource"))
        app_name_source = normalize_str(row.get("app_name_source"))

        billing_id = normalize_str(row.get("billing_owner_appsvcid"))
        support_id = normalize_str(row.get("support_owner_appsvcid"))

        # ---------- Non-orphaned ----------
        if is_orphan == 0:
            if owner_source == "resourcetag":
                return "APM via Resource Owner Tag"
            if owner_source == "rginherited":
                return "APM via RG Tag Inference"

            # If tags explicitly say "name"/"email" → naming pattern
            if "name" in app_name_source or "email" in app_name_source:
                return "APM via Naming Pattern"

            # default: mapped through AppSvcID
            return "APM via Owner AppsvcID"

        # ---------- Orphaned ----------
        # if final ID is missing OR obviously bad text => unmapped
        if pd.isna(final_id_raw) or final_id in invalid_ids:
            return "unmapped"

        # Good AppSvcID from tags (or billing/support) → Tag ID
        if billing_id.startswith(("app", "bsn")) or support_id.startswith(
            ("app", "bsn")
        ):
            return "APM via Resource Tag ID"

        # Other cases (e.g., AppSvcID is a name like 'SANTOSH BOMPALLY')
        return "APM via Naming Pattern"

    final_df["ownership_determination_method"] = final_df.apply(
        determine_method, axis=1
    )

    def determine_confidence(row: pd.Series) -> int:
        """
        Simple: method → score
        - APM via Resource Owner Tag / Resource Tag ID: 100
        - APM via RG Tag Inference: 80
        - APM via Naming Pattern: 60
        - unmapped / everything else: 0
        """
        method = normalize_str(row.get("ownership_determination_method"))

        if "naming pattern" in method:
            return 60
        if "rg tag inference" in method:
            return 80
        if "resource owner tag" in method or "resource tag id" in method:
            return 100
        return 0

    final_df["ownership_confidence_score"] = final_df.apply(
        determine_confidence, axis=1
    )

    # ==================================================
    # F. Orphan flags & reasons
    # ==================================================

    # Keep original flag for debugging and re-compute after mapping  <<< NEW
    final_df["original_is_orphaned"] = final_df["isOrphaned"].fillna(0).astype(int)

    def compute_final_orphan_flag(row: pd.Series) -> int:
        orig = int(row.get("original_is_orphaned") or 0)
        if orig == 0:
            return 0  # was never orphaned

        final_id = normalize_str(row.get("final_app_service_id"))
        app_name = normalize_str(row.get("application_name"))
        owner_name = normalize_str(row.get("billing_owner_name"))

        has_valid_appsvc = final_id.startswith(("app", "bsn"))
        has_person_or_app = bool(app_name or owner_name)

        # If we now have a good AppSvcID + app/owner → treat as resolved
        if has_valid_appsvc and has_person_or_app:
            return 0

        return 1

    final_df["is_orphaned"] = final_df.apply(compute_final_orphan_flag, axis=1)

    def determine_orphan_reason(row: pd.Series) -> Optional[str]:
        """
        Simple categories for true orphans only.
        """
        if row["is_orphaned"] == 0:
            return None

        final_id = normalize_str(row.get("final_app_service_id"))
        snow_appid = row.get("AppID")

        # If mapped into Snow AppID but still flagged orphan -> resolved via tags
        if pd.notna(snow_appid):
            return "resolved_via_tags"

        if not final_id:
            return "no_snow_mapping"

        if not final_id.startswith(("app", "bsn")):
            return "invalid_appsvcid"

        return "no_snow_mapping"

    final_df["orphan_reason"] = final_df.apply(determine_orphan_reason, axis=1)

    # ==================================================
    # G. Hash / audit / timestamps
    # ==================================================
    now_utc = pd.Timestamp.utcnow()

    final_df["mapping_created_date"] = now_utc
    final_df["is_current"] = True
    final_df["resource_created_date"] = final_df.get("FirstSeenDate")
    final_df["last_verified_date"] = final_df.get("LastVerifiedDate").fillna(now_utc)
    final_df["last_modified_date"] = final_df.get("LastModifiedDate").fillna(now_utc)

    def hash_row(row: pd.Series) -> str:
        parts = [
            str(row.get("ResourceId") or ""),
            str(row.get("final_app_service_id") or ""),
            str(row.get("application_name") or ""),
            str(row.get("billing_owner_email") or ""),
        ]
        return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()

    final_df["hash_key"] = final_df.apply(hash_row, axis=1)
    final_df["u_id"] = [str(uuid.uuid4()) for _ in range(len(final_df))]
    final_df["audit_id"] = [str(uuid.uuid4()) for _ in range(len(final_df))]

    # ==================================================
    # H. Resource type standardisation
    # ==================================================
    final_df["resource_type_standardized"] = final_df["ResourceType"].apply(
        categorize_resource_type
    )

    # ==================================================
    # I. Rename + select Gold 5.1 columns
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
        # you can also keep "original_is_orphaned" for debugging if you want:
        # "original_is_orphaned",
    ]

    final_df = final_df[[c for c in final_columns if c in final_df.columns]]

    return final_df


# ================================================================
# 5. Example main (for testing in Jupyter)
# ================================================================
if _name_ == "_main_":  # <<< CHANGED: fixed main guard
    # TODO: replace with real connection details / env vars
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

    # Optional: write a small sample to CSV for validation in SSMS
    # gold_df.head(1000).to_csv(r"C:\temp\gold_5_1_sample.csv", index=False)