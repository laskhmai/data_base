def fetch_data(sql_query: str) -> pd.DataFrame:
    try:
        print("[DEBUG] Executing SQL query:\n", sql_query)
        conn = connect_to_db()
        data = pd.read_sql(sql_query, conn)
        print(f"[DEBUG] Query returned {data.shape[0]} rows and {data.shape[1]} columns.")
        conn.close()
        return data
    except Exception as exc:
        print(f"Error fetching data: {exc}")
        return pd.DataFrame()