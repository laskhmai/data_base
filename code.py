# Auto-detect available date range
date_sql = """
    SELECT
        MIN(_date) AS MinDate,
        MAX(_date) AS MaxDate
    FROM [Metrics].[MongoDBRightsizingAggregated5Min]
"""
date_df = fetch_data(date_sql)
start_date_dt = date_df["MinDate"].iloc[0]
end_date_dt   = date_df["MaxDate"].iloc[0]

months        = [end_date_dt.strftime("%Y-%m")]
start_dates   = [start_date_dt.strftime("%Y-%m-%d")]
end_dates     = [end_date_dt.strftime("%Y-%m-%d")]