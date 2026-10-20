"""MongoDB Atlas Direct Invoice API POC.

Two org names -> TXT credentials -> discover org IDs -> Aug/Sep/Oct 2026
invoice JSON and CSV -> daily summaries -> optional SQL staging.

IMPORTANT: Invoice CSV Amount is not equivalent to FOCUS BilledCost.
Invoice-level credits cannot be assigned to resource rows without evidence.
"""
import csv
import io
import logging
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import requests
import pyodbc
from requests.auth import HTTPDigestAuth

# ============ EDIT ONLY THIS CONFIGURATION ============
ORGANIZATION_NAMES = ["YOUR_FIRST_ORG_NAME", "YOUR_SECOND_ORG_NAME"]
KEY_FILE = Path(r"C:\YOUR_PATH\MongoDBKeys 1.txt")
MONTHS = [(2026, 8), (2026, 9), (2026, 10)]
LOAD_TO_SQL = False  # Start with False; only True after checking results.
# Explicit second safety switch: duplicate prevention is not yet implemented.
ALLOW_APPEND_WITH_DUPLICATES = False
DB_CONFIG = {
    "server": "YOUR_SQL_SERVER",
    "database": "YOUR_DATABASE",
    "username": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD",
    "driver": "ODBC Driver 17 for SQL Server",
}
TABLE = "[Staging].[MongoDB_DirectInvoice]"
BASE = "https://cloud.mongodb.com/api/atlas/v2"
JSON_ACCEPT = "application/vnd.atlas.2025-03-12+json"
CSV_ACCEPT = "application/vnd.atlas.2023-01-01+csv"
COLUMNS = [
    "ChargePeriodStart", "SubAccountId", "SubAccountName", "ResourceId",
    "ResourceType", "ServiceName", "RegionName", "BilledCost", "EffectiveCost",
    "ChargeCategory", "PricingCategory", "SkuMeter", "ServiceCategory",
    "ServiceSubcategory", "InvoiceId", "OriginalAmount", "CreditAmount",
]
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("direct_invoice")


def normalized(name):
    return (name or "").strip().lower()


def read_keys(path):
    result = {}
    with path.open(encoding="utf-8-sig") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(";", 2)
            if len(parts) != 3 or not all(parts[i].startswith(p) for i, p in enumerate(("Org-", "Public-", "Private-"))):
                raise ValueError(f"Incorrect credential format on line {line_no}")
            name, public, private = (parts[0][4:].strip(), parts[1][7:].strip(), parts[2][8:].strip())
            if not all((name, public, private)):
                raise ValueError(f"Empty credential on line {line_no}")
            if normalized(name) in result:
                raise ValueError(f"Duplicate org in TXT at line {line_no}")
            result[normalized(name)] = (public, private)
    return result


def get_json(session, path, params=None):
    r = session.get(BASE + path, params=params, headers={"Accept": JSON_ACCEPT}, timeout=120)
    r.raise_for_status()
    return r.json()


def find_org_id(session, org_name):
    page = 1
    while True:
        data = get_json(session, "/orgs", {"itemsPerPage": 500, "pageNum": page})
        entries = data.get("results", [])
        for org in entries:
            if normalized(org.get("name")) == normalized(org_name):
                return org["id"]
        if not entries or len(entries) < 500 or page * 500 >= data.get("totalCount", 10**9):
            break
        page += 1
    raise ValueError(f"Org not found or inaccessible: {org_name}")


def associated_invoice_ids(session, org_id, year, month):
    data = get_json(session, f"/orgs/{org_id}/associatedInvoices", {"year": year, "month": month, "includeLinkedOrgs": "false"})
    ids = []
    for item in data.get("associatedInvoices", []):
        # Do not silently attribute a linked org's invoice to this org.
        if item.get("orgId") == org_id and item.get("invoiceId"):
            ids.append(item["invoiceId"])
    return list(dict.fromkeys(ids))


def invoice_json(session, org_id, invoice_id):
    return get_json(session, f"/orgs/{org_id}/invoices/{invoice_id}")


def invoice_csv(session, org_id, invoice_id):
    r = session.get(
        BASE + f"/orgs/{org_id}/invoices/{invoice_id}/csv",
        headers={"Accept": CSV_ACCEPT}, timeout=180,
    )
    r.raise_for_status()
    return r.content.decode("utf-8-sig")


def money(value):
    if value is None or str(value).strip() == "":
        return None
    try:
        return Decimal(str(value).replace(",", "").replace("$", "").strip())
    except InvalidOperation as exc:
        raise ValueError(f"Unexpected monetary value: {value!r}") from exc


def cents_to_dollars(value):
    return (Decimal(str(value)) / Decimal("100")) if value is not None else None


def parse_date(value):
    if not value:
        return None
    s = str(value).strip()
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d")
        except ValueError:
            return None
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def read_invoice_csv(text):
    """Atlas invoice CSV contains a metadata preamble followed by Date,Usage Date,... header."""
    lines = text.splitlines()
    header_at = None
    for i, line in enumerate(lines):
        cells = next(csv.reader([line]), [])
        normalized_cells = [c.strip().lower() for c in cells]
        if "usage date" in normalized_cells and "amount" in normalized_cells:
            header_at = i
            break
    if header_at is None:
        raise ValueError("Invoice CSV line-item header not found; inspect the API CSV format")
    reader = csv.DictReader(io.StringIO("\n".join(lines[header_at:])))
    return [r for r in reader if r.get("Amount", "").strip()]


def map_row(source, invoice_id):
    """Preserve invoice Amount as OriginalAmount; do not manufacture net cost or credits."""
    usage = parse_date(source.get("Usage Date") or source.get("Date"))
    return {
        "ChargePeriodStart": usage,
        "SubAccountId": source.get("Project ID") or None,
        "SubAccountName": source.get("Project") or None,
        "ResourceId": None,  # Cluster name is not necessarily a unique FOCUS ResourceId.
        "ResourceType": None,
        "ServiceName": source.get("Description") or None,
        "RegionName": source.get("Region") or None,
        "BilledCost": None,  # Invoice CSV Amount is not FOCUS BilledCost.
        "EffectiveCost": None,
        "ChargeCategory": None,
        "PricingCategory": None,
        "SkuMeter": source.get("SKU") or None,
        "ServiceCategory": None,
        "ServiceSubcategory": None,
        "InvoiceId": invoice_id,
        "OriginalAmount": money(source.get("Amount")),
        "CreditAmount": None,  # Invoice-level credits must not be spread across line items.
    }


def summarize(org_name, year, month, invoice_id, details, raw, rows):
    print(f"\n{'='*78}\n{org_name} | invoice period {year}-{month:02d} | invoice {invoice_id}")
    print(f"Invoice status: {details.get('statusName', 'unknown')}")
    for field in ("subtotalCents", "amountBilledCents", "creditsCents", "amountPaidCents", "salesTaxCents"):
        val = cents_to_dollars(details.get(field))
        if val is not None:
            print(f"{field}: ${val:.2f}")
    print(f"Invoice CSV line items: {len(raw)}")
    total = sum((r["OriginalAmount"] or Decimal(0) for r in rows), Decimal(0))
    print(f"CSV Amount sum (invoice units): {total:.4f}")
    daily = defaultdict(lambda: Decimal(0))
    unknown_dates = 0
    for row in rows:
        if row["ChargePeriodStart"] is None:
            unknown_dates += 1
        else:
            daily[row["ChargePeriodStart"].date()] += row["OriginalAmount"] or Decimal(0)
    for day, amount in sorted(daily.items()):
        print(f"  {day} | Amount={amount:.4f}")
    if unknown_dates:
        print(f"Rows without a parsed usage date: {unknown_dates}")
    print("NOTE: Invoice-level credits are NOT allocated to individual resources.")
    print("NOTE: CSV Amount and invoice cents may use different billing units; do not equate them.")


def connect_sql():
    c = DB_CONFIG
    return pyodbc.connect(
        f"DRIVER={{{c['driver']}}};SERVER={c['server']};DATABASE={c['database']};"
        f"UID={c['username']};PWD={c['password']};Encrypt=yes;TrustServerCertificate=no;",
        autocommit=False,
    )


def insert_rows(conn, rows):
    if not rows:
        return 0
    cols = ", ".join(f"[{c}]" for c in COLUMNS)
    marks = ", ".join("?" for _ in COLUMNS)
    sql = f"INSERT INTO {TABLE} ({cols}) VALUES ({marks})"
    cur = conn.cursor()
    try:
        cur.executemany(sql, [tuple(row[c] for c in COLUMNS) for row in rows])
        conn.commit()
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def main():
    if LOAD_TO_SQL and not ALLOW_APPEND_WITH_DUPLICATES:
        raise RuntimeError(
            "SQL append is blocked: no deduplication strategy yet. "
            "Review data and schema first; explicitly set ALLOW_APPEND_WITH_DUPLICATES=True "
            "only if you accept duplicate risk."
        )
    keys = read_keys(KEY_FILE)
    sql_conn = connect_sql() if LOAD_TO_SQL else None
    try:
        for org_name in ORGANIZATION_NAMES:
            auth = keys.get(normalized(org_name))
            if not auth:
                log.error("No TXT credentials match org name %s", org_name)
                continue
            with requests.Session() as session:
                session.auth = HTTPDigestAuth(*auth)
                try:
                    org_id = find_org_id(session, org_name)
                    log.info("Found organization ID for %s: %s", org_name, org_id)
                except (requests.RequestException, ValueError) as err:
                    log.error("Org lookup failed for %s: %s", org_name, type(err).__name__)
                    continue
                for year, month in MONTHS:
                    try:
                        ids = associated_invoice_ids(session, org_id, year, month)
                        if not ids:
                            log.warning("No finalized associated invoice for %s %04d-%02d", org_name, year, month)
                            continue
                        for invoice_id in ids:
                            details = invoice_json(session, org_id, invoice_id)
                            raw = read_invoice_csv(invoice_csv(session, org_id, invoice_id))
                            rows = [map_row(item, invoice_id) for item in raw]
                            summarize(org_name, year, month, invoice_id, details, raw, rows)
                            if sql_conn:
                                log.info("Inserted %s records", insert_rows(sql_conn, rows))
                            else:
                                log.info("DRY RUN: SQL insert skipped")
                    except (requests.RequestException, ValueError, RuntimeError, pyodbc.Error) as err:
                        log.error("Failed %s %04d-%02d: %s", org_name, year, month, type(err).__name__)
                        # Stop if SQL writes fail; do not silently continue with partial load.
                        if sql_conn:
                            raise
    finally:
        if sql_conn:
            sql_conn.close()


if __name__ == "__main__":
    main()
