def load_sources(account_name: str | None = None):
    """
    Load resource, virtual tags, and SNOW data.

    If account_name is provided, limit resources (and previous hashes)
    to that subscription only.
    """
    # 1) Active resources from vw_ActiveResources (Silver)
    base_sql = """
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
            [BillingOwnerAppsvcid],
            [SupportOwnerAppsvcid],
            [OwnerSource],
            [IsPlatformManaged],
            [ManagementModel],
            [IsOrphaned],
            [HasConflictingTags],
            [DependencyTriggeredUpdate],
            [TagQualityScore],
            [ResTags],
            [HashKey],
            [ChangeCategory],
            [FirstSeenDate]
        FROM [silver].[vw_ActiveResources]
    """

    params: list = []
    if account_name:
        base_sql += " WHERE [AccountName] = ?"
        params.append(account_name)

    with connect(lenticular_server, lenticular_database,
                 lenticular_username, lenticular_password) as con_l:
        resources_df = read_sql_df(con_l, base_sql, params=params)

    # 2) Virtual tags – same as before
    with connect(hybrid1_server, hybrid1_database,
                 hybrid1_username, hybrid1_password) as con_h:
        virtual_tags_df = read_sql_df(con_h, """
            SELECT *
            FROM [Gold].[InferredTags]
        """)

    # 3) SNOW / EAPM – same as before
    with connect(hybrid1_server, hybrid1_database,
                 hybrid1_username, hybrid1_password) as con_h:
        snow_df = read_sql_df(con_h, """
            SELECT *
            FROM [Silver].[SnowAppsNormalized]
        """)

    return resources_df, virtual_tags_df, snow_df


def get_subscriptions() -> list[str]:
    """
    Get distinct AccountName values from the active resources view.
    """
    sql = """
        SELECT DISTINCT [AccountName]
        FROM [silver].[vw_ActiveResources]
    """
    with connect(lenticular_server, lenticular_database,
                 lenticular_username, lenticular_password) as con:
        df = read_sql_df(con, sql)
    return df['AccountName'].dropna().tolist()


def load_previous_hashes(account_name: str) -> pd.DataFrame:
    """
    Get previous HashKey for each ResourceId for this subscription
    from the normalized table (current snapshot).
    """
    sql = """
        SELECT
            [ResourceId],
            [HashKey] AS [HashKey_Gold]
        FROM [Silver].[AzureResourcesNormalized]
        WHERE [AccountName] = ?
          AND [IsCurrent] = 1
    """
    with connect(lenticular_server, lenticular_database,
                 lenticular_username, lenticular_password) as con:
        return read_sql_df(con, sql, params=[account_name])


def filter_changed_resources(resources_df: pd.DataFrame,
                             prev_hash_df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only rows that are new or whose HashKey changed.
    """
    if prev_hash_df.empty:
        # First run → process everything
        return resources_df

    merged = resources_df.merge(
        prev_hash_df[['ResourceId', 'HashKey_Gold']],
        on='ResourceId',
        how='left'
    )

    is_new = merged['HashKey_Gold'].isna()
    is_changed = merged['HashKey_Gold'].notna() & (merged['HashKey'] != merged['HashKey_Gold'])

    return merged.loc[is_new | is_changed, resources_df.columns]


def process_subscription(account_name: str):
    print(f"\n=== Processing subscription: {account_name} ===")

    # 1) Load sources for this subscription
    resources_df, virtual_tags_df, snow_df = load_sources(account_name)

    if resources_df.empty:
        print("No resources for this subscription. Skipping.")
        return

    # 2) Load previous hashes for this subscription and filter delta
    prev_hash_df = load_previous_hashes(account_name)
    delta_df = filter_changed_resources(resources_df, prev_hash_df)

    if delta_df.empty:
        print("No new or changed resources (hash unchanged). Skipping transform.")
        return

    # 3) Transform only the delta
    print(f"Transforming {len(delta_df)} resources...")
    gold_df = transform(delta_df, virtual_tags_df, snow_df)

    if gold_df is None or gold_df.empty:
        print("Transform returned no rows. Skipping insert.")
        return

    # 4) Load to staging and call stored procedure
    staging_table_name = "[AZURE].[GoldResourcesNormalizedStagingFinal_1]"
    store_proc_name    = "[Gold].[usp_AzureResourceNormalized]"

    print(f"Loading {len(gold_df)} rows to staging...")
    insert_gold_parallel(
        gold_df,
        staging_table=staging_table_name,
        post_proc=store_proc_name,
        batch_size=1000,
        max_workers=4,
    )


def main():
    subs = get_subscriptions()
    print(f"Found {len(subs)} subscriptions: {subs}")

    for account_name in subs:
        process_subscription(account_name)


# Run the workflow
main()


def read_sql_df(con, sql, params=None):
    cursor = con.cursor()

    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)

    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]

    return pd.DataFrame.from_records(rows, columns=columns)
