def write_metrics_to_db(cursor, averaged, cluster_key, project_key):
    """
    Batch inserts CPU metric datapoints into
    [Metrics].[MongoDB_Cpu_User_15M]
    """
    if averaged is None or averaged.empty:
        return

    insert_query = """
        INSERT INTO [Metrics].[MongoDB_Cpu_User_15M] 
            ([Key], [Measurement], [Datetime], [ClusterKey], [ProjectKey])
        VALUES (NEWID(), ?, ?, ?, ?)
    """

    cpu_data = averaged[
        averaged["metric_name"] == "PROCESS_NORMALIZED_CPU_USER"
    ]

    rows_inserted = 0
    for _, row in cpu_data.iterrows():
        cursor.execute(insert_query, (
            row["average_value"],
            row["time_bucket"],
            cluster_key,
            project_key
        ))
        rows_inserted += 1

    logger.info(f"{rows_inserted} CPU rows inserted for ClusterKey={cluster_key}")