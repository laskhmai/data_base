
import csv
import io
import os
import time
import zipfile
import logging
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
from collections import defaultdict

import requests
import pyodbc
from requests.auth import HTTPDigestAuth


# ============================================================
# CONFIGURATION - FILL THESE VALUES ON COMPANY LAPTOP
# ============================================================

ORGANIZATIONS = {
    "YOUR_FIRST_ORG_NAME": "YOUR_FIRST_ORG_ID",
    "YOUR_SECOND_ORG_NAME": "YOUR_SECOND_ORG_ID",
}

# Existing TXT file format:
# Org-Name;Public-xxxxx;Private-xxxxx
KEY_FILE = Path(r"C:\YOUR_PATH\MongoDBKeys 1.txt")

# Two completed calendar months
MONTHS = [
    (2026, 8),
    (2026, 9),
]

# First run: False means no database inserts
LOAD_TO_SQL = False

DB_CONFIG = {
    "server": "YOUR_SQL_SERVER",
    "database": "YOUR_DATABASE",
    "username": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD",
    "driver": "ODBC Driver 17 for SQL Server",
}

TABLE_NAME = "[Staging].[MongoDB_FOCUS]"

BASE_URL = "https://cloud.mongodb.com/api/atlas/v2"
ATLAS_ACCEPT = "application/vnd.atlas.2025-03-12+json"

COLUMNS = [
    "ChargePeriodStart",
    "SubAccountId",
    "SubAccountName",
    "ResourceId",
    "ResourceType",
    "ServiceName",
    "RegionName",
    "BilledCost",
    "EffectiveCost",
    "ChargeCategory",
    "PricingCategory",
    "SkuMeter",
    "ServiceCategory",
    "ServiceSubcategory",
]


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("MongoDB_FOCUS")


# ============================================================
# STEP 1 - READ EXISTING TXT CREDENTIALS
# ============================================================

def normalize_org_name(name):
    return (name or "").strip().lower()


def read_keys_from_file(file_path):
    """
    Reads existing credential format:

    Org-OrganizationName;Public-xxxxx;Private-xxxxx

    Does not print secrets.
    """

    auth_lookup = {}

    with open(file_path, "r", encoding="utf-8-sig") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            parts = line.split(";", 2)

            if len(parts) != 3:
                raise ValueError(
                    f"Invalid credential format at line {line_number}"
                )

            org_part, public_part, private_part = parts

            if not org_part.startswith("Org-"):
                raise ValueError(
                    f"Missing Org- prefix at line {line_number}"
                )

            if not public_part.startswith("Public-"):
                raise ValueError(
                    f"Missing Public- prefix at line {line_number}"
                )

            if not private_part.startswith("Private-"):
                raise ValueError(
                    f"Missing Private- prefix at line {line_number}"
                )

            org_name = org_part[len("Org-"):].strip()
            public_key = public_part[len("Public-"):].strip()
            private_key = private_part[len("Private-"):].strip()

            if not all([org_name, public_key, private_key]):
                raise ValueError(
                    f"Empty credential value at line {line_number}"
                )

            key = normalize_org_name(org_name)

            if key in auth_lookup:
                raise ValueError(
                    f"Duplicate organization at line {line_number}"
                )

            auth_lookup[key] = {
                "org_name": org_name,
                "public_key": public_key,
                "private_key": private_key,
            }

    return auth_lookup


# ============================================================
# STEP 2 - CREATE AUTHENTICATED MONGODB SESSION
# ============================================================

def create_session(auth_entry):
    session = requests.Session()

    session.auth = HTTPDigestAuth(
        auth_entry["public_key"],
        auth_entry["private_key"]
    )

    session.headers.update({
        "Accept": ATLAS_ACCEPT
    })

    return session


def api_json(session, method, url, **kwargs):
    response = session.request(
        method,
        url,
        timeout=120,
        **kwargs
    )

    response.raise_for_status()

    return response.json()


# ============================================================
# STEP 3 - FIND INVOICE FOR MONTH
# ============================================================

def get_invoice_id(session, org_id, year, month):

    url = (
        f"{BASE_URL}/orgs/{org_id}/associatedInvoices"
    )

    result = api_json(
        session,
        "GET",
        url,
        params={
            "month": month,
            "year": year
        }
    )

    invoices = result.get("associatedInvoices", [])

    matching = [
        item for item in invoices
        if item.get("orgId") == org_id
    ]

    if len(matching) != 1:
        raise RuntimeError(
            f"Expected exactly one invoice for "
            f"{org_id} / {year}-{month:02d}. "
            f"Found: {len(matching)}"
        )

    invoice_id = matching[0].get("invoiceId")

    if not invoice_id:
        raise RuntimeError("Invoice ID missing in API response")

    return invoice_id


# ============================================================
# STEP 4 - REQUEST FOCUS REPORT
# ============================================================

def create_focus_report(session, org_id, invoice_id):

    url = (
        f"{BASE_URL}/orgs/{org_id}"
        f"/invoices/{invoice_id}/reports"
    )

    payload = {
        "reportType": "FOCUS",
        "reportFormat": "CSV",
        "formatSpecVersion": "1.3"
    }

    result = api_json(
        session,
        "POST",
        url,
        json=payload
    )

    report_id = result.get("reportId")

    if not report_id:
        raise RuntimeError("Report ID missing in API response")

    return report_id


# ============================================================
# STEP 5 - WAIT FOR REPORT GENERATION
# ============================================================

def wait_for_report(session, org_id, invoice_id, report_id):

    url = (
        f"{BASE_URL}/orgs/{org_id}"
        f"/invoices/{invoice_id}/reports/{report_id}"
    )

    for attempt in range(40):

        result = api_json(session, "GET", url)

        state = result.get("state")

        logger.info("Report state: %s", state)

        if state == "SUCCEEDED":
            download_url = result.get("downloadUrl")

            if not download_url:
                raise RuntimeError(
                    "Report succeeded but download URL is missing"
                )

            return download_url

        if state == "FAILED":
            raise RuntimeError(
                f"FOCUS report generation failed: "
                f"{result.get('failureReason', 'Unknown reason')}"
            )

        if state not in ("PENDING", "RUNNING"):
            raise RuntimeError(
                f"Unexpected report state: {state}"
            )

        time.sleep(10)

    raise TimeoutError(
        "FOCUS report was not ready within polling window"
    )


# ============================================================
# STEP 6 - DOWNLOAD REPORT INTO MEMORY
# ============================================================

def download_focus_csv(download_url):

    # Presigned URL; do not attach MongoDB API credentials.
    response = requests.get(
        download_url,
        timeout=180
    )

    response.raise_for_status()

    content = response.content

    if zipfile.is_zipfile(io.BytesIO(content)):

        with zipfile.ZipFile(io.BytesIO(content)) as archive:

            csv_files = [
                name for name in archive.namelist()
                if name.lower().endswith(".csv")
                and not name.startswith("__MACOSX/")
            ]

            if len(csv_files) != 1:
                raise RuntimeError(
                    f"Expected one CSV inside report ZIP. "
                    f"Found: {csv_files}"
                )

            content = archive.read(csv_files[0])

    return content.decode("utf-8-sig")


# ============================================================
# STEP 7 - DATA CONVERSION
# ============================================================

def parse_datetime(value):

    if not value:
        return None

    dt = datetime.fromisoformat(
        str(value).strip().replace("Z", "+00:00")
    )

    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)

    return dt.replace(tzinfo=None)


def parse_decimal(value):

    if value is None or str(value).strip() == "":
        return None

    return Decimal(str(value).replace(",", ""))


def get_value(source, *field_names):

    for field_name in field_names:
        value = source.get(field_name)

        if value is not None and str(value).strip():
            return str(value).strip()

    return None


# ============================================================
# STEP 8 - MAP FOCUS CSV TO 14 STAGING COLUMNS
# ============================================================

def map_focus_row(source):

    return {
        "ChargePeriodStart": parse_datetime(
            source.get("ChargePeriodStart")
        ),

        "SubAccountId": get_value(
            source, "SubAccountId"
        ),

        "SubAccountName": get_value(
            source, "SubAccountName"
        ),

        # Not confirmed in the tested FOCUS CSV.
        "ResourceId": None,
        "ResourceType": None,

        "ServiceName": get_value(
            source, "ServiceName"
        ),

        "RegionName": get_value(
            source, "RegionName"
        ),

        # Already provided in billing currency.
        # Do NOT multiply by 0.7 again.
        "BilledCost": parse_decimal(
            source.get("BilledCost")
        ),

        "EffectiveCost": parse_decimal(
            source.get("EffectiveCost")
        ),

        "ChargeCategory": get_value(
            source, "ChargeCategory"
        ),

        "PricingCategory": None,

        "SkuMeter": get_value(
            source, "SkuMeter", "SkuId"
        ),

        "ServiceCategory": get_value(
            source, "ServiceCategory"
        ),

        "ServiceSubcategory": None,
    }


# ============================================================
# STEP 9 - READ CSV AND FILTER USAGE MONTH
# ============================================================

def read_focus_rows(csv_text, year, month):

    reader = csv.DictReader(io.StringIO(csv_text))

    required_fields = {
        "ChargePeriodStart",
        "BilledCost",
        "EffectiveCost",
        "SubAccountId",
        "SubAccountName",
    }

    available_fields = set(reader.fieldnames or [])

    missing_fields = required_fields - available_fields

    if missing_fields:
        raise RuntimeError(
            f"Missing required FOCUS fields: {missing_fields}"
        )

    mapped_rows = []
    source_count = 0
    excluded_count = 0

    for source in reader:

        source_count += 1

        mapped = map_focus_row(source)

        usage_date = mapped["ChargePeriodStart"]

        if usage_date is None:
            excluded_count += 1
            continue

        # Important:
        # MongoDB invoices can contain previous-month usage.
        # Keep only usage belonging to the requested month.
        if (
            usage_date.year != year
            or usage_date.month != month
        ):
            excluded_count += 1
            continue

        mapped_rows.append(mapped)

    return mapped_rows, source_count, excluded_count


# ============================================================
# STEP 10 - SHOW DAILY COST SUMMARY
# ============================================================

def print_daily_summary(org_name, year, month, rows):

    daily = defaultdict(
        lambda: {
            "count": 0,
            "billed": Decimal("0"),
            "effective": Decimal("0")
        }
    )

    for row in rows:

        usage_date = row["ChargePeriodStart"].date()

        daily[usage_date]["count"] += 1

        daily[usage_date]["billed"] += (
            row["BilledCost"] or Decimal("0")
        )

        daily[usage_date]["effective"] += (
            row["EffectiveCost"] or Decimal("0")
        )

    print("\n" + "=" * 75)
    print(f"ORGANIZATION: {org_name}")
    print(f"MONTH: {year}-{month:02d}")
    print("=" * 75)

    for usage_date, data in sorted(daily.items()):

        print(
            f"{usage_date} | "
            f"Rows={data['count']} | "
            f"BilledCost=${data['billed']:.2f} | "
            f"EffectiveCost=${data['effective']:.2f}"
        )

    total_billed = sum(
        (row["BilledCost"] or Decimal("0"))
        for row in rows
    )

    total_effective = sum(
        (row["EffectiveCost"] or Decimal("0"))
        for row in rows
    )

    print("-" * 75)
    print("Total records:", len(rows))
    print(f"Total BilledCost: ${total_billed:.2f}")
    print(f"Total EffectiveCost: ${total_effective:.2f}")


# ============================================================
# STEP 11 - CONNECT TO SQL SERVER
# ============================================================

def connect_to_db():

    config = DB_CONFIG

    connection_string = (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )

    return pyodbc.connect(
        connection_string,
        autocommit=False
    )


# ============================================================
# STEP 12 - INSERT INTO FOCUS STAGING TABLE
# ============================================================

def insert_focus_rows(connection, rows):

    if not rows:
        return 0

    columns_sql = ", ".join(
        f"[{column}]" for column in COLUMNS
    )

    placeholders = ", ".join(
        "?" for _ in COLUMNS
    )

    insert_sql = (
        f"INSERT INTO {TABLE_NAME} "
        f"({columns_sql}) "
        f"VALUES ({placeholders})"
    )

    values = [
        tuple(row[column] for column in COLUMNS)
        for row in rows
    ]

    cursor = connection.cursor()

    try:
        cursor.executemany(insert_sql, values)
        connection.commit()

        return len(values)

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()


# ============================================================
# MAIN - PROCESS TWO ORGANIZATIONS / TWO MONTHS
# ============================================================

def main():

    logger.info("MongoDB FOCUS POC started")

    auth_lookup = read_keys_from_file(KEY_FILE)

    logger.info(
        "Credential file loaded successfully"
    )

    connection = None

    if LOAD_TO_SQL:
        connection = connect_to_db()
        logger.info("Connected to SQL Server")

    try:

        for org_name, org_id in ORGANIZATIONS.items():

            normalized_name = normalize_org_name(org_name)

            auth_entry = auth_lookup.get(normalized_name)

            if not auth_entry:
                logger.error(
                    "Credentials not found for organization: %s",
                    org_name
                )
                continue

            logger.info(
                "Processing organization: %s",
                org_name
            )

            with create_session(auth_entry) as session:

                for year, month in MONTHS:

                    logger.info(
                        "Fetching %s / %04d-%02d",
                        org_name, year, month
                    )

                    try:

                        invoice_id = get_invoice_id(
                            session, org_id, year, month
                        )

                        report_id = create_focus_report(
                            session, org_id, invoice_id
                        )

                        download_url = wait_for_report(
                            session,
                            org_id,
                            invoice_id,
                            report_id
                        )

                        csv_text = download_focus_csv(
                            download_url
                        )

                        rows, source_count, excluded_count = (
                            read_focus_rows(
                                csv_text, year, month
                            )
                        )

                        logger.info(
                            "Source rows=%s, "
                            "Selected rows=%s, "
                            "Excluded rows=%s",
                            source_count,
                            len(rows),
                            excluded_count
                        )

                        print_daily_summary(
                            org_name, year, month, rows
                        )

                        if LOAD_TO_SQL:

                            inserted = insert_focus_rows(
                                connection, rows
                            )

                            logger.info(
                                "Inserted %s rows into %s",
                                inserted, TABLE_NAME
                            )

                        else:
                            logger.info(
                                "DRY RUN - SQL insertion skipped"
                            )

                    except Exception as error:
                        logger.error(
                            "FAILED for %s / %04d-%02d: %s",
                            org_name,
                            year,
                            month,
                            error
                        )

    finally:

        if connection:
            connection.close()

    logger.info("MongoDB FOCUS POC completed")


if __name__ == "__main__":
    main()
