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
