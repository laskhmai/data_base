mport pyodbc
from datetime import date, timedelta

# ---------- DB CONNECTION ----------
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=your_server_name;"
    "DATABASE=your_database_name;"
    "UID=your_username;"
    "PWD=your_password;"
    "TrustServerCertificate=yes;"
)
cursor = conn.cursor()

# ---------- DATE RANGE ----------
start_date = date(2025, 12, 31)
end_date = date(2023, 1, 1)

current_date = start_date

# ---------- LOOP THROUGH EACH DATE ----------
while current_date >= end_date:
    day = current_date.day
    month = current_date.month
    year = current_date.year

    # ---------- STORED PROCEDURE CALL ----------
    cursor.execute(
        """
        EXEC your_stored_procedure_name
            @day = ?,
            @month = ?,
            @year = ?
        """,
        day, month, year
    )

    conn.commit()

    print(f"Executed for {day}-{month}-{year}")

    current_date -= timedelta(days=1)

# ---------- CLEANUP ----------
cursor.close()
conn.close()