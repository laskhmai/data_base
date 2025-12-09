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
