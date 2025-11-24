def insert_gold(gold_df: pd.DataFrame, batch_size: int = 50000):
    """
    Fast-ish bulk insert using pyodbc + fast_executemany.
    Assumes connection details are already known.
    """
    # 1) Replace NaN with None and build rows via itertuples (faster than values.tolist())
    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in gold_df.itertuples(index=False, name=None)
    ]

    insert_sql = f"""
    INSERT INTO [AZURE].[GoldResourcesNormalizedStagingfinal_1] (
        {', '.join(f'[{col}]' for col in gold_df.columns)}
    )
    VALUES (
        {', '.join('?' for _ in gold_df.columns)}
    );
    """

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={hybrid_server};"
        f"DATABASE={hybrid_database};"
        f"UID={hybrid_username};"
        f"PWD={hybrid_password};"
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



    # ==================================================
    # E. Owner / email / platform team in ONE pass
    # ==================================================

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

        # Platform team name (same rule you had before)
        platform_team = None
        val = str(row.get("isPlatformManaged", "")).strip().lower()
        if val in ("0", "true"):  # 0 = platform-managed in your data
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







    # ==================================================
    # F. Ownership method, confidence, orphan flags & hash in ONE pass
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

        # ---------- ownership method ----------
        if orig_orphan == 0:
            if owner_source == "resourcetag":
                method = "APM via Resource Owner Tag"
            elif owner_source == "rginherited":
                method = "APM via RG Tag Inference"
            elif "name" in app_name_source or "email" in app_name_source:
                method = "APM via Naming Pattern"
            else:
                method = "APM via Owner AppsvcID"
        else:
            if pd.isna(final_id_raw) or final_id in invalid_ids:
                method = "unmapped"
            elif billing_id.startswith(("app", "bsn")) or support_id.startswith(
                ("app", "bsn")
            ):
                method = "APM via Resource Tag ID"
            else:
                method = "APM via Naming Pattern"

        # ---------- confidence ----------
        method_norm = normalize_str(method)
        if "naming pattern" in method_norm:
            confidence = 60
        elif "rg tag inference" in method_norm:
            confidence = 80
        elif "resource owner tag" in method_norm or "resource tag id" in method_norm:
            confidence = 100
        else:
            confidence = 0

        # ---------- final orphan flag ----------
        if orig_orphan == 0:
            final_orphan = 0
        else:
            app_name = normalize_str(row.get("application_name"))
            owner_name = normalize_str(row.get("billing_owner_name"))
            has_valid_appsvc = final_id.startswith(("app", "bsn"))
            has_person_or_app = bool(app_name or owner_name)

            if has_valid_appsvc and has_person_or_app:
                final_orphan = 0
            else:
                final_orphan = 1

        # ---------- orphan reason ----------
        orphan_reason = None
        if final_orphan == 1:
            snow_appid = row.get("AppID")
            if pd.notna(snow_appid):
                orphan_reason = "resolved_via_tags"
            elif not final_id:
                orphan_reason = "no_snow_mapping"
            elif not final_id.startswith(("app", "bsn")):
                orphan_reason = "invalid_appsvcid"
            else:
                orphan_reason = "no_snow_mapping"

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
                hash_key,
            ],
            index=[
                "ownership_determination_method",
                "ownership_confidence_score",
                "is_orphaned",
                "orphan_reason",
                "hash_key",
            ],
        )

    ownership_fields = final_df.apply(compute_ownership_fields, axis=1)
    final_df[ownership_fields.columns] = ownership_fields


method_norm = normalize_str(method)
    if "naming pattern" in method_norm:
        confidence = 60
    elif "rg tag inference" in method_norm:
        confidence = 80
    elif "resource owner tag" in method_norm or "resource tag id" in method_norm:
        confidence = 100
    else:
        confidence = 0

    # NEW: override confidence to 100 when we have BOTH AppSvcID and AppID
    # final_id is your AppSvcID (from tags/4.1)
    has_valid_appsvc = final_id.startswith(("app", "bsn"))

    # billing_owner_appid already combines Snow.AppID + tags.app_id
    appid_raw = row.get("billing_owner_appid") or row.get("AppID") or row.get("app_id")
    has_appid = bool(normalize_str(appid_raw))

    if has_valid_appsvc and has_appid:
        confidence = 100