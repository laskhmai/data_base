import requests
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus


def initialize_azure_connection(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    scope: str = "https://management.azure.com/.default",
) -> str:
    """
    Get Azure AD OAuth2 access token using client credentials flow.
    """
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope,
    }

    response = requests.post(token_url, data=payload, timeout=30)
    response.raise_for_status()
    return response.json()["access_token"]


def extract_data(api_url: str, token: str, params: dict | None = None) -> list[dict]:
    """
    Call Azure API with Bearer token and return records.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    response = requests.get(api_url, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    # Common Azure API pattern: payload in 'value'
    if isinstance(data, dict) and "value" in data:
        return data["value"]
    if isinstance(data, list):
        return data
    return [data]


def transform_data(
    api_records: list[dict],
    column_mapper: dict[str, str],
    filter_column: str | None = None,
    filter_values: list | None = None,
) -> pd.DataFrame:
    """
    Convert API response to DataFrame, filter rows, and map API fields -> DB columns.
    """
    df = pd.json_normalize(api_records)

    # Optional row filtering
    if filter_column and filter_values is not None:
        df = df[df[filter_column].isin(filter_values)]

    # Keep only mapped columns and rename to DB-compatible names
    available_source_cols = [c for c in column_mapper.keys() if c in df.columns]
    df = df[available_source_cols].rename(columns=column_mapper)

    return df


def load_to_sql_server(
    df: pd.DataFrame,
    server: str,
    database: str,
    username: str,
    password: str,
    table_name: str,
    batch_size: int = 1000,
    driver: str = "ODBC Driver 18 for SQL Server",
):
    """
    Load DataFrame to Microsoft SQL Server in batches.
    """
    if df.empty:
        print("No records to load.")
        return

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}",
        fast_executemany=True,
    )

    with engine.begin() as conn:
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists="append",
            index=False,
            chunksize=batch_size,
            method="multi",
        )

    print(f"Loaded {len(df)} records into {table_name}.")


# Example usage
if _name_ == "_main_":
    tenant_id = "your-tenant-id"
    client_id = "your-client-id"
    client_secret = "your-client-secret"

    api_url = "https://management.azure.com/subscriptions/<sub-id>/resources?api-version=2021-04-01"

    column_mapper = {
        "id": "resource_id",
        "name": "resource_name",
        "type": "resource_type",
        "location": "region",
    }

    token = initialize_azure_connection(tenant_id, client_id, client_secret)
    records = extract_data(api_url, token)
    transformed_df = transform_data(
        records,
        column_mapper=column_mapper,
        filter_column="location",
        filter_values=["eastus", "westus2"],
    )

    load_to_sql_server(
        transformed_df,
        server="your-sql-server.database.windows.net",
        database="your_db",
        username="your_user",
        password="your_password",
        table_name="dbo.azure_resources",
        batch_size=1000,
    )
    