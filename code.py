# Check if az3-udap-prd records
# exist anywhere in Gold Active

sql_gold_active = """
    SELECT COUNT(*) as count
    FROM [Gold].[AzureActiveResourceOwnerShipNormalized]
    WHERE cloud_account_name = 'az3-udap-prd'
"""

with connect(hybrideasi_server,
             hybrideasi_database,
             hybrideasi_username,
             hybrideasi_password) as con_h:
    result = read_sql_df(con_h, sql_gold_active)

print(f"az3-udap-prd in Gold Active: {result}")