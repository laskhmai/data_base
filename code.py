"""MongoDB Atlas FOCUS invoice report POC (August and September 2026).

Install: pip install requests pyodbc
Credentials TXT format: Org-OrganizationName;Public-API_KEY;Private-API_SECRET

First run with LOAD_TO_SQL=False. No SQL writes in preview mode.
"""

import csv
import io
import logging
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import requests
from requests.auth import HTTPDigestAuth

# ================ EDIT THIS SECTION ================
ORGANIZATION_NAMES = [
    "YOUR_FIRST_ORG_NAME",
    "YOUR_SECOND_ORG_NAME",
]
KEY_FILE = Path(r"C:\YOUR_PATH\MongoDBKeys 1.txt")
MONTHS = [(2026, 8), (2026, 9)]
LOAD_TO_SQL = False  # Set True ONLY after preview/validation and dedupe planning.
DB_CONFIG = {
    "server": "YOUR_SQL_SERVER",
    "database": "YOUR_DATABASE",
    "username": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD",
    "driver": "ODBC Driver 17 for SQL Server",
}
# ===================================================

BASE_URL = "https://cloud.mongodb.com/api/atlas/v2"
ACCEPT = "application/vnd.atlas.2025-03-12+json"
TABLE_NAME = "[Staging].[MongoDB_FOCUS]"
COLUMNS = [
    "ChargePeriodStart", "SubAccountId", "SubAccountName", "ResourceId",
    "ResourceType", "ServiceName", "RegionName", "BilledCost",
    "EffectiveCost", "ChargeCategory", "PricingCategory", "SkuMeter",
    "ServiceCategory", "ServiceSubcategory",
]
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("mongodb-focus")


def normalize(value):
    return (value or "").strip().casefold()


def read_keys_from_file(path):
    if not path.is_file():
        raise FileNotFoundError(f"Credentials file not found: {path}")
    lookup = {}
    with path.open("r", encoding="utf-8-sig") as stream:
        for number, line in enumerate(stream, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(";", 2)
            if len(parts) != 3 or not all(parts[i].startswith(prefix) for i, prefix in enumerate(("Org-", "Public-", "Private-"))):
                raise ValueError(f"Invalid credentials TXT format at line {number}")
            name = parts[0][4:].strip()
            public_key = parts[1][7:].strip()
            private_key = parts[2][8:].strip()
            if not (name and public_key and private_key):
                raise ValueError(f"Missing credential value at line {number}")
            if normalize(name) in lookup:
                raise ValueError(f"Duplicate organization name in TXT: {name}")
            lookup[normalize(name)] = (public_key, private_key)
    return lookup


def atlas_session(keys):
    session = requests.Session()
    session.auth = HTTPDigestAuth(*keys)
    session.headers.update({"Accept": ACCEPT})
    return session


def atlas_json(session, method, url, **kwargs):
    response = session.request(method, url, timeout=120, **kwargs)
    response.raise_for_status()
    return response.json()


def get_org_id(session, organization_name):
    """Find matching org ID; follow pagination instead of assuming first page."""
    url = f"{BASE_URL}/orgs"
    page = 1
    while True:
        payload = atlas_json(session, "GET", url, params={"pageNum": page, "itemsPerPage": 100})
        results = payload.get("results", [])
        for org in results:
            if normalize(org.get("name")) == normalize(organization_name):
                if not org.get("id"):
                    raise RuntimeError(f"Organization found without ID: {organization_name}")
                return org["id"]
        if not results or len(results) < 100:
            break
        page += 1
    raise RuntimeError(f"Organization '{organization_name}' not found among organizations accessible to this API key")


def get_invoice_id(session, org_id, year, month):
    payload = atlas_json(
        session, "GET", f"{BASE_URL}/orgs/{org_id}/associatedInvoices",
        params={"month": month, "year": year},
    )
    invoices = payload.get("associatedInvoices", [])
    if not invoices:
        raise RuntimeError(f"No associated invoice found for {year}-{month:02d}")
    # Atlas may return a billing organization's invoice associated with this org.
    invoice_ids = sorted({str(item.get("invoiceId")) for item in invoices if item.get("invoiceId")})
    if len(invoice_ids) != 1:
        raise RuntimeError(f"Expected one distinct associated invoice for {year}-{month:02d}; found {len(invoice_ids)}. Inspect invoice metadata before choosing.")
    return invoice_ids[0]


def create_report(session, org_id, invoice_id):
    payload = atlas_json(
        session, "POST", f"{BASE_URL}/orgs/{org_id}/invoices/{invoice_id}/reports",
        json={"reportType": "FOCUS", "reportFormat": "CSV", "formatSpecVersion": "1.3"},
    )
    report_id = payload.get("reportId")
    if not report_id:
        raise RuntimeError("Report response missing reportId")
    return report_id


def wait_for_report(session, org_id, invoice_id, report_id):
    url = f"{BASE_URL}/orgs/{org_id}/invoices/{invoice_id}/reports/{report_id}"
    for _ in range(40):
        payload = atlas_json(session, "GET", url)
        status = str(payload.get("state") or payload.get("status") or "").upper()
        if status in ("SUCCEEDED", "COMPLETED", "SUCCESS"):
            download_url = payload.get("downloadUrl")
            if not download_url:
                raise RuntimeError("Report succeeded but downloadUrl is missing")
            return download_url
        if status in ("FAILED", "ERROR", "CANCELLED"):
            raise RuntimeError(f"Report generation ended with status {status}")
        if status not in ("PENDING", "RUNNING", "IN_PROGRESS", "QUEUED", "PROCESSING"):
            raise RuntimeError(f"Unrecognized report state: {status or '<missing>'}")
        time.sleep(10)
    raise TimeoutError("Report did not complete within polling window")


def download_csv(download_url):
    # A presigned download URL must NOT receive Atlas Digest credentials.
    response = requests.get(download_url, timeout=180)
    response.raise_for_status()
    blob = response.content
    if zipfile.is_zipfile(io.BytesIO(blob)):
        with zipfile.ZipFile(io.BytesIO(blob)) as archive:
            csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv") and not name.startswith("__MACOSX/")]
            if len(csv_names) != 1:
                raise RuntimeError(f"Expected one CSV in report ZIP, found {len(csv_names)}")
            blob = archive.read(csv_names[0])
    return blob.decode("utf-8-sig")


def parse_date(value):
    if not value or not value.strip():
        return None
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def parse_money(value):
    if value is None or not str(value).strip():
        return None
    try:
        return Decimal(str(value).strip().replace(",", ""))
    except InvalidOperation as exc:
        raise ValueError("Invalid decimal in FOCUS CSV") from exc


def field(row, *names):
    for name in names:
        value = row.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def map_row(source):
    return {
        "ChargePeriodStart": parse_date(field(source, "ChargePeriodStart")),
        "SubAccountId": field(source, "SubAccountId"),
        "SubAccountName": field(source, "SubAccountName"),
        "ResourceId": None,
        "ResourceType": None,
        "ServiceName": field(source, "ServiceName"),
        "RegionName": field(source, "RegionName"),
        "BilledCost": parse_money(field(source, "BilledCost")),
        "EffectiveCost": parse_money(field(source, "EffectiveCost")),
        "ChargeCategory": field(source, "ChargeCategory"),
        "PricingCategory": None,
        # Confirm actual CSV header during first run; do not silently use an unrelated SKU.
        "SkuMeter": field(source, "SkuMeter", "SkuId"),
        "ServiceCategory": field(source, "ServiceCategory"),
        "ServiceSubcategory": None,
    }


def parse_focus_csv(csv_text, year, month):
    reader = csv.DictReader(io.StringIO(csv_text))
    headers = reader.fieldnames or []
    required = {"ChargePeriodStart", "BilledCost", "EffectiveCost", "SubAccountId", "SubAccountName"}
    missing = required - set(headers)
    if missing:
        raise RuntimeError(f"FOCUS CSV missing required headers: {sorted(missing)}")
    log.info("FOCUS CSV headers: %s", ", ".join(headers))
    records, total, excluded = [], 0, 0
    for source in reader:
        total += 1
        mapped = map_row(source)
        date = mapped["ChargePeriodStart"]
        if date is None:
            raise RuntimeError(f"Missing ChargePeriodStart at CSV row {total + 1}")
        if (date.year, date.month) != (year, month):
            excluded += 1
            continue
        records.append(mapped)
    return records, total, excluded


def print_summary(name, year, month, rows):
    daily = defaultdict(lambda: [0, Decimal("0"), Decimal("0")])
    for row in rows:
        day = row["ChargePeriodStart"].date()
        daily[day][0] += 1
        daily[day][1] += row["BilledCost"] or Decimal("0")
        daily[day][2] += row["EffectiveCost"] or Decimal("0")
    print(f"\n{'=' * 75}\n{name} | {year}-{month:02d}\n{'=' * 75}")
    for day, (count, billed, effective) in sorted(daily.items()):
        print(f"{day} | Rows: {count:5d} | Billed: {billed:,.2f} | Effective: {effective:,.2f}")
    print(f"TOTAL | Rows: {len(rows)} | Billed: {sum((r['BilledCost'] or Decimal('0') for r in rows), Decimal('0')):,.2f} | Effective: {sum((r['EffectiveCost'] or Decimal('0') for r in rows), Decimal('0')):,.2f}")
    print("NOTE: FOCUS costs are not proof of final invoice amount after credits.")


def connect_to_sql():
    import pyodbc
    c = DB_CONFIG
    connection_string = (
        f"DRIVER={{{c['driver']}}};SERVER={c['server']};DATABASE={c['database']};"
        f"UID={c['username']};PWD={c['password']};Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(connection_string, autocommit=False)


def insert_rows(connection, rows):
    if not rows:
        return 0
    col_names = ", ".join(f"[{name}]" for name in COLUMNS)
    placeholders = ", ".join("?" for _ in COLUMNS)
    query = f"INSERT INTO {TABLE_NAME} ({col_names}) VALUES ({placeholders})"
    cursor = connection.cursor()
    try:
        cursor.executemany(query, [tuple(row[name] for name in COLUMNS) for row in rows])
        connection.commit()
        return len(rows)
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def main():
    if len(ORGANIZATION_NAMES) != 2 or any(name.startswith("YOUR_") for name in ORGANIZATION_NAMES):
        raise ValueError("Fill exactly two organization names in ORGANIZATION_NAMES")
    if len({normalize(name) for name in ORGANIZATION_NAMES}) != 2:
        raise ValueError("Organization names must be different")
    auth_lookup = read_keys_from_file(KEY_FILE)
    for name in ORGANIZATION_NAMES:
        if normalize(name) not in auth_lookup:
            raise ValueError(f"No matching Org- entry in TXT for: {name}")

    # Gather all reports before writing; failures will not create partial inserts.
    pending = []
    failures = []
    for org_name in ORGANIZATION_NAMES:
        with atlas_session(auth_lookup[normalize(org_name)]) as session:
            try:
                org_id = get_org_id(session, org_name)
                log.info("Found org ID for %s: %s", org_name, org_id)
            except Exception as exc:
                log.error("Organization lookup failed for %s: %s", org_name, type(exc).__name__)
                failures.append((org_name, "org lookup"))
                continue
            for year, month in MONTHS:
                log.info("Fetching %s / %04d-%02d", org_name, year, month)
                try:
                    invoice_id = get_invoice_id(session, org_id, year, month)
                    report_id = create_report(session, org_id, invoice_id)
                    url = wait_for_report(session, org_id, invoice_id, report_id)
                    rows, total, excluded = parse_focus_csv(download_csv(url), year, month)
                    log.info("Invoice rows=%s; matching usage-month rows=%s; excluded=%s", total, len(rows), excluded)
                    print_summary(org_name, year, month, rows)
                    pending.append((org_name, year, month, rows))
                except Exception as exc:
                    # Avoid exposing API response bodies or credential-bearing URLs in logs.
                    log.error("Failed %s / %04d-%02d (%s)", org_name, year, month, type(exc).__name__)
                    failures.append((org_name, f"{year}-{month:02d}"))

    if failures:
        log.warning("Some reports failed: %s", failures)
    if not LOAD_TO_SQL:
        log.info("PREVIEW ONLY: No SQL inserts. Successful reports: %s", len(pending))
        return
    if failures or len(pending) != len(ORGANIZATION_NAMES) * len(MONTHS):
        raise RuntimeError("SQL load blocked because one or more organization-month reports failed")
    raise RuntimeError(
        "SQL load intentionally blocked: this 14-column table has no source Org ID, "
        "invoice ID, or deduplication key. Add source lineage and idempotent loading "
        "before enabling database writes."
    )


if __name__ == "__main__":
    main()
