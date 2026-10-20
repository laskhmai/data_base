
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
