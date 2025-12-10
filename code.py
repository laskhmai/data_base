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
