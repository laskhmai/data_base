def read_sql_df(con, sql: str,
                params: Optional[Iterable[Any]] = None) -> pd.DataFrame:
    cursor = con.cursor()
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description] \
                  if cursor.description else []
        return pd.DataFrame.from_records(rows, columns=columns)
    finally:
        cursor.close()

# ✅ FIX 2: New retry wrapper — add this right after read_sql_df
def read_sql_df_with_retry(con_factory, sql: str,
                            params=None,
                            retries: int = 3,
                            delay: int = 10) -> pd.DataFrame:
    """
    Retries SQL execution up to `retries` times on 
    connection failure. delay increases each attempt.
    """
    last_error = None
    for attempt in range(retries):
        try:
            with con_factory() as con:
                return read_sql_df(con, sql, params)
        except pyodbc.OperationalError as e:
            last_error = e
            if attempt < retries - 1:
                wait = delay * (attempt + 1)
                print(f"Connection failed attempt "
                      f"{attempt+1}/{retries}. "
                      f"Retrying in {wait}s... Error: {e}")
                time.sleep(wait)
            else:
                print(f"All {retries} attempts failed.")
                raise
    raise last_error