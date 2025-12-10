def insert_gold_parallel(
    gold_df: pd.DataFrame,
    staging_table: str,
    batch_size: int = 1000,
    max_workers: int = 4,
    truncate: bool = True,
    post_proc: Optional[str] = None
):
    """
    Parallel batch insertion into staging table.

    truncate=True  -> TRUNCATE staging_table before insert.
    post_proc      -> stored procedure name to EXEC after insert (or None).
    """

    # use your existing server/db/user/password/driver here – DO NOT change this line
    connection_factory = make_connection_factory(
        hybrideasi_server,
        hybrideasi_database,
        hybrideasi_username,
        hybrideasi_password,
        ODBC_DRIVER,
    )

    # 1) TRUNCATE only when asked
    if truncate:
        truncate_sql = f"TRUNCATE TABLE {staging_table};"
        print(f"Truncating table {staging_table}...")
        execute_non_query(connection_factory, truncate_sql, autocommit=True)
        print("Truncate complete.")

    # 2) Prepare rows
    rows = gold_df.where(pd.notna(gold_df), None).values.tolist()

    insert_sql = f"""
    INSERT INTO {staging_table} (
        {', '.join(f'[{col}]' for col in gold_df.columns)}
    ) VALUES (
        {', '.join('?' for _ in gold_df.columns)}
    )
    """

    # 3) Split into batches
    batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

    # 4) Run in parallel
    print(f"Starting parallel insert of {len(rows)} rows in {len(batches)} batches using {max_workers} workers...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(insert_batch, batch, insert_sql, connection_factory) 
                   for batch in batches]
        for future in as_completed(futures):
            print(future.result())

    print("All batches processed successfully.")

    # 5) Execute stored procedure **only if provided**
    if post_proc:
        exec_proc_sql = f"EXEC {post_proc}"
        print(f"Executing stored procedure: {post_proc} ...")
        execute_non_query(connection_factory, exec_proc_sql, autocommit=False)
        print("Stored procedure execution complete.")
staging_table_name = "[AZURE].[InactiveAzureGoldResourcesNormalized]"

insert_gold_parallel(
    gold_df,
    staging_table_name,
    batch_size=1000,
    max_workers=4,
    truncate=False,   # don’t truncate each subscription
    post_proc=None    # don’t execute stored proc
)
def main():
    subs = get_subscriptions()
    print(f"Found {len(subs)} subscriptions: {subs}")

    start_from = "az3-mulesoft-npe"   # the one that failed

    # find its position
    start_index = subs.index(start_from)

    # process only from this one onwards
    for account_name in subs[start_index:]:
        process_subscription(account_name)

main()

def main():
    subs = get_subscriptions()
    print(f"Found {len(subs)} subscriptions: {subs}")

    start_from = "az3-mulesoft-npe"   # the one that failed

    if start_from in subs:
        start_index = subs.index(start_from)
        print(f"Resuming from subscription: {start_from}")
    else:
        print(f"{start_from} not found in list. Starting from beginning.")
        start_index = 0

    for account_name in subs[start_index:]:
        process_subscription(account_name)

main()



def insert_batch(batch, insert_sql, connection_factory):
    try:
        with connection_factory(autocommit=False) as con:
            cur = con.cursor()
            cur.fast_executemany = True
            cur.executemany(insert_sql, batch)
            con.commit()
        return f"Inserted {len(batch)} rows"
    except Exception as e:
        # DO NOT return "Error". Raise so caller can stop.
        raise RuntimeError(f"Batch insert failed: {e}") from e


def insert_gold_parallel(gold_df: pd.DataFrame, staging_table, batch_size=1000, max_workers=4):
    connection_factory = make_connection_factory(
        hybridesa1_server, hybridesa1_database, hybridesa1_username, hybridesa1_password, ODBC_DRIVER
    )

    # (optional) truncate, build insert_sql, rows, batches... same as before

    batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]
    print(f"Starting parallel insert of {len(rows)} rows in {len(batches)} batches using {max_workers} workers...")

    # ✅ NEW ERROR-AWARE BLOCK
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(insert_batch, batch, insert_sql, connection_factory)
            for batch in batches
        ]

        errors = []
        for future in as_completed(futures):
            try:
                msg = future.result()
                print(msg)
            except Exception as e:
                errors.append(e)
                print("Batch failed:", e)

        if errors:
            raise RuntimeError(
                f"One or more batches failed while inserting into {staging_table}"
            )

    print("All batches processed successfully.")
    # then your stored proc / next steps



START

1. Get all subscription names
   subs = SELECT DISTINCT AccountName FROM Silver.ActiveResources

2. Loop each subscription
   FOR each subscription in subs:

       PRINT "Processing subscription:", subscription

3. Load Silver resources for this subscription
       resources = SELECT * 
                   FROM Silver.ActiveResources
                   WHERE AccountName = subscription

       IF resources is empty:
           PRINT "No resources. Skip."
           CONTINUE

4. Load supporting datasets (full tables)
       virtual_tags = SELECT * FROM Gold.VirtualTags
       snow_data    = SELECT * FROM Silver.SNOW_AppData

5. Load previous Gold hashkeys (for same subscription)
       previous_hash = SELECT ResourceId, HashKey
                       FROM Gold.GoldResources
                       WHERE AccountName = subscription

6. Identify NEW or CHANGED rows
       FOR each row in resources:
           IF ResourceId NOT in previous_hash → mark as NEW
           ELSE IF row.HashKey != previous_hash[row.ResourceId] → mark as CHANGED
           ELSE → mark as UNCHANGED

       delta_rows = NEW + CHANGED rows

       IF delta_rows is empty:
           PRINT "No change. Skip transformation."
           CONTINUE

7. Transform delta rows (ownership/orphan logic)
       FOR each row in delta_rows:
           IF row.is_orphaned = 0:
               resolve ownership using SNOW
           ELSE:
               IF resource exists in virtual_tags:
                   resolve using virtual tags
               ELSE:
                   mark as orphan (method="Orphan_NoTags")

8. Compute final hashkey + timestamps
       FOR each row:
           final_hashkey = SHA256(important fields)
           last_modified_date = NOW()
           processing_date = NOW()

9. Insert transformed rows into GOLD staging
       INSERT delta_rows INTO Gold.GoldResourcesStagingFinal

10. End subscription and continue with next

END
Here are the columns we fetch from SNOW after joining with the
Virtual Tagging table:

• AppServiceID / AppID
• AppName
• EAPMID
• AppOwnerEmail
• AppOwnerName
• BusinessUnit
• Department

Virtual Tagging gives inferred application details, but SNOW provides
the authoritative application and ownership info. That’s why we still
join SNOW to enrich the final record.




FUNCTION transform(resource_df, virtual_tags_df, snow_df):

    ----------------------------------------------------------------
    STEP 0: Pre-work – build helper sets and normalize keys
    ----------------------------------------------------------------
    valid_eapm_ids = unique lower-cased EapmId values from snow_df

    # normalize resource_id for joins
    resource_df.resource_id_key = lower(str(ResourceId))
    virtual_tags_df.resource_id_key = lower(str(resource_id))

    ----------------------------------------------------------------
    STEP 1: Split resources into orphan vs non-orphan
    ----------------------------------------------------------------
    # isOrphaned is 1/0, fill NULLs with 0
    orphan_mask        = (resource_df.isOrphaned filled with 0 == 1)
    orphaned_res_df    = rows where orphan_mask == TRUE
    non_orphaned_res_df= rows where orphan_mask == FALSE

    ----------------------------------------------------------------
    STEP 2: Extract primary AppServiceID from resource table
    ----------------------------------------------------------------
    # for non-orphan resources only
    non_orphaned_res_df.primary_appservice = 
        apply pick_app_service_id(row):
            1. get "primary_appservice" column
            2. normalize string (strip, lower)
            3. return that value (may be empty)

    ----------------------------------------------------------------
    STEP 3: Merge inferred tags (virtual tags) onto non-orphan resources
    ----------------------------------------------------------------
    non_orphaned_with_tags = LEFT JOIN
        non_orphaned_res_df
        WITH virtual_tags_df
        ON resource_id_key
        (brings columns: app_service_id, inferred_app_name, scores, res_tags, etc.)

    ----------------------------------------------------------------
    STEP 4: Determine final AppServiceID and source for non-orphan
    ----------------------------------------------------------------
    For each row in non_orphaned_with_tags:

        # helper pick_non_orphaned_appsvc(row):
        primary_id = normalized primary_appservice
        tag_id     = normalized app_service_id from tags
        final_tag  = normalized final_app_service_id (if exists)

        IF primary_id is not empty:
            return primary_id  # primary wins
        ELSE IF tag_id not empty:
            return tag_id      # tag based
        ELSE IF final_tag not empty:
            return final_tag
        ELSE:
            return NULL

    non_orphaned_with_tags.final_app_service_id =
        apply pick_non_orphaned_appsvc

    non_orphaned_with_tags.appsvc_source =
        apply infer_appsvc_source_non_orphan(row):
            - compare primary vs tag vs final
            - return "resource table" or "tags_table" or "None"

    ----------------------------------------------------------------
    STEP 5: Merge SNOW metadata (app owners) using AppServiceID / EAPM
    ----------------------------------------------------------------
    # Build small lookup from snow_df
    snow_appsvc = snow_df[
        EapmId, AppOwner, AppOwnerEmail, BusinessUnit, Department
    ]
    normalize key:
        snow_appsvc.eapm_key = lower(str(EapmId))
        drop duplicates on eapm_key

    # Build mapping from eapm_key -> owner info
    eapm_name_lookup  = dict(eapm_key -> AppOwner)
    eapm_email_lookup = dict(eapm_key -> AppOwnerEmail)
    eapm_bu_lookup    = dict(eapm_key -> BusinessUnit)
    eapm_dept_lookup  = dict(eapm_key -> Department)

    # normalize appsvc ids in final_df for lookup
    final_df = non_orphaned_with_tags + orphaned_res_df combined
    final_df.billing_owner_appsvcid  = normalize_str(billing_owner_appsvcid)
    final_df.support_owner_appsvcid  = normalize_str(support_owner_appsvcid)

    # create eapm_key in final_df (based on final_app_service_id)
    final_df.eapm_key = normalize_str(final_app_service_id)

    # where we have a valid eapm_key and resource is orphaned:
    mask_eapm = eapm_key not null AND isOrphaned == 1

    # fill OWNER NAME from EAPM for orphaned rows
    final_df.billing_owner_name[mask_eapm] =
        lookup eapm_name_lookup[eapm_key] when present
    final_df.support_owner_name[mask_eapm] =
        same logic as billing owner

    # fill OWNER EMAIL from EAPM
    final_df.billing_owner_email[mask_eapm] =
        lookup eapm_email_lookup[eapm_key]
    final_df.support_owner_email[mask_eapm] =
        lookup eapm_email_lookup[eapm_key]

    # fill BU + Department from EAPM as backup fields
    final_df.business_unit[mask_eapm] =
        lookup eapm_bu_lookup[eapm_key] if empty
    final_df.department[mask_eapm] =
        lookup eapm_dept_lookup[eapm_key] if empty

    ----------------------------------------------------------------
    STEP 6: Fix emails and platform team labels
    ----------------------------------------------------------------
    # normalize emails with small helper:
    snow_email_lookup = dict(AppOwnerEmail -> AppOwner normalized)

    For each row:
        if billing_owner_name looks like an email AND exists in lookup:
            replace with proper name
        same for support_owner_name

    # platform team name:
    final_df.platform_team_name = 
        IF isPlatformManaged is true THEN support_owner_name ELSE NULL

    ----------------------------------------------------------------
    STEP 7: Ownership determination + confidence + orphan reason
    ----------------------------------------------------------------
    For each row in final_df:

        original_orphan = isOrphaned before ownership logic

        final_id_raw   = final_app_service_id
        billing_id     = normalized billing_owner_appsvcid
        support_id     = normalized support_owner_appsvcid
        final_id_norm  = normalize_str(final_id_raw)

        method        = None
        confidence    = 0
        final_orphan  = original_orphan
        orphan_reason = None

        # CASE 1: Direct EAPMID match in SNOW
        IF final_id_norm is numeric AND in valid_eapm_ids:
            method        = "Resource Tags EAPMID"
            confidence    = 100
            final_orphan  = 0
            orphan_reason = None

        # CASE 2: app / bsn tag match
        ELSE IF billing_id or support_id starts with "app" or "bsn"
               AND confidence < some threshold:
            method        = "Virtual Tagging Resource Tag"
            confidence    = 60
            final_orphan  = 0
            orphan_reason = None

        # CASE 3: looks like naming/email pattern but not in SNOW
        ELSE IF final_id_norm matches app/email pattern:
            method        = "Virtual Tagging Naming Pattern"
            confidence    = 40
            final_orphan  = 2
            orphan_reason = "invalid_eapm_id"

        # CASE 4: still unmapped
        ELSE:
            method        = None
            confidence    = 0
            final_orphan  = 2 or 3 (depending)
            orphan_reason = "NoTag"

        # Non-orphan override logic:
        IF owner_source == "resourcetags":
            method     = "ResourceTags"
            confidence = 100
            final_orphan = 0
        ELSE IF owner_source == "inherited":
            method     = "ParentTags"
            confidence = 60

        # Save fields into row
        set:
            ownership_determination_method = method
            ownership_confidence_score     = confidence
            is_orphaned                    = final_orphan
            orphan_reason                  = orphan_reason

    ----------------------------------------------------------------
    STEP 8: Compute hash key for change detection
    ----------------------------------------------------------------
    For each row in final_df:

        parts = [
            ResourceId,
            Region,
            Environment,
            ApplicationName,
            BillingOwnerEmail,
            BillingOwnerAppsvcid,
            BillingOwnerName,
            SupportOwnerEmail,
            SupportOwnerAppsvcid,
            SupportOwnerName,
            BusinessUnit,
            Department,
            ownership_determination_method,
            ownership_confidence_score,
            orphan_reason,
            isPlatformManaged,
            isOrphaned
        ]  # all as strings, empty when NULL

        hash_key = SHA256(join_with('\n', parts))

        store as column: hash_key

    ----------------------------------------------------------------
    STEP 9: Add timestamps and standard columns
    ----------------------------------------------------------------
    now_utc = current UTC timestamp

    final_df.mapping_created_date = now_utc
    final_df.is_current           = TRUE
    final_df.first_seen_date      = FirstSeenDate (filled with now_utc if null)
    final_df.last_verified_date   = LastVerifiedDate (or now_utc)
    final_df.last_modified_date   = LastModifiedDate (or now_utc)

    # generate mapping_id and guid for each row
    final_df.mapping_id = new UUID for each row
    final_df.guid       = new UUID for each row

    ----------------------------------------------------------------
    STEP 10: Resource type standardization + rename to Gold schema
    ----------------------------------------------------------------
    final_df.resource_type_standardized =
        apply categorize_resource_type(ResourceType)

    # Rename columns from raw names to Gold names:
    - "ResourceId"  -> "resource_id"
    - "ResourceName"-> "resource_name"
    - "CloudProvider" -> "cloud_provider"
    - "CloudAccountId"-> "cloud_account_id"
    - ...
    - keep all ownership + hash + timestamps

    final_columns = [
        "resource_id",
        "resource_name",
        "resource_type",
        "resource_type_standardized",
        "cloud_provider",
        "cloud_account_id",
        "account_name",
        "region",
        "environment",
        "billing_owner_appsvcid",
        "billing_owner_name",
        "billing_owner_email",
        "support_owner_appsvcid",
        "support_owner_name",
        "support_owner_email",
        "appsvc_source",
        "final_app_service_id",
        "business_unit",
        "department",
        "platform_team_name",
        "management_model",
        "is_platform_managed",
        "is_deleted",
        "is_orphaned",
        "orphan_reason",
        "ownership_determination_method",
        "ownership_confidence_score",
        "has_conflicting_tags",
        "dependency_triggered_update",
        "res_tags",
        "hash_key",
        "first_seen_date",
        "last_verified_date",
        "last_modified_date",
        "mapping_created_date",
        "is_current"
        ... (any other audit columns you kept)
    ]

    RETURN final_df[final_columns]
END FUNCTION