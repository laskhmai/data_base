def load_child_tables() -> Tuple[pd.DataFrame, pd.DataFrame]:
    
    # ✅ FIX: Use retry for both connections
    lenticular_factory = make_connection_factory(
        lenticular_server, lenticular_database,
        lenticular_username, lenticular_password, ODBC_DRIVER
    )
    hybrideasi_factory = make_connection_factory(
        hybrideasi_server, hybrideasi_database,
        hybrideasi_username, hybrideasi_password, ODBC_DRIVER
    )
    
    vt_sql = """
        SELECT
            [resource_id], [resource_name],
            [resource_group_name], [subscription_id],
            [resource_type], [cloud_provider],
            [subscription_name], [environment],
            [change_category], [res_tags], [data_hash],
            [inferred_app_name], [app_name_source],
            [confidence_score], [app_owner],
            [ml_app_name], [ml_app_owner], [app_pattern],
            [app_id], [app_id_source_tag], [app_service_id],
            [rg_inferred_app_name], [processing_date]
        FROM [Gold].[VTag_Azure_InferredTags]
    """
    
    snow_sql = """
        SELECT
            COALESCE([AppServiceID], [APPID]) AS [AppServiceID],
            [AppID], [AppName], [EapmId],
            [AppOwnerEmail], [AppOwner],
            [BusinessUnit], [Department]
        FROM [Silver].[SnowNormalized]
    """
    
    # ✅ Retry on connection failure
    virtual_tags_df = read_sql_df_with_retry(
        lenticular_factory, vt_sql, retries=3, delay=10
    )
    snow_df = read_sql_df_with_retry(
        hybrideasi_factory, snow_sql, retries=3, delay=10
    )
    
    return virtual_tags_df, snow_df