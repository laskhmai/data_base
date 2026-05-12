def main():
    """
    MAIN ENTRY POINT
    This function is executed when the script runs in prod.
    """

    # ── GUARD 1: Check KEY_LIST is not empty ────────────────────────
    if not KEY_LIST:
        logger.error(
            "KEY_LIST is empty — no orgs configured to process. "
            "Please add org slugs to KEY_LIST before running."
        )
        return

    # ── Load all org keys from Azure Key Vault ───────────────────────
    auth_keys = read_keys_from_vault(KEY_LIST)

    # ── GUARD 2: Check keys were actually loaded ─────────────────────
    if not auth_keys:
        logger.error(
            "No MongoDB Atlas auth keys were loaded from Key Vault. "
            "Check that secrets exist in kv-hybridautomation and "
            "follow the naming convention: "
            "{org}-public-key / {org}-private-key"
        )
        return

    # ── Build lookup map: OrgName alias -> auth entry ────────────────
    auth_lookup = {}
    for entry in auth_keys:
        for alias in entry.get("aliases", {entry["org"]}):
            auth_lookup[alias]                     = entry
            auth_lookup[canonical_org_name(alias)] = entry

    # ── Connect to SQL database ──────────────────────────────────────
    connection = connect_to_db()
    cursor     = connection.cursor()

    try:
        organizations = read_org_table(cursor)
        cluster_map   = read_cluster_map(cursor)

        for org in organizations:

            # Reset process counter for each org
            total_processes = 0

            # Match org to Key Vault auth entry
            normalized_name = normalize_org_name(org["Name"])
            canonical_name  = canonical_org_name(org["Name"])
            auth_entry = (
                auth_lookup.get(normalized_name)
                or auth_lookup.get(canonical_name)
            )

            # No auth found for this org — log and skip
            if not auth_entry:
                logger.error(
                    f"FAILED | "
                    f"OrgId={org['OrgId']} | "
                    f"OrgName={org['Name']} | "
                    f"ProjectId=N/A | "
                    f"ProcessId=N/A | "
                    f"Reason=No auth entry found in Key Vault"
                )
                continue

            # Read all projects for this org
            projects = read_project_table(cursor, org["OrgKey"])

            for project in projects:
                try:
                    # Fetch processes from Atlas API
                    processes = get_project_processes(
                        project["ProjectId"],
                        auth_entry,
                        org["OrgKey"],
                        project["ProjectKey"],
                    )

                    # Accumulate total processes for this org
                    total_processes += len(processes)

                    # Write to database
                    write_processes_to_db(cursor, processes, cluster_map)
                    cursor.connection.commit()

                except Exception as e:
                    # Failure log with full context
                    logger.error(
                        f"FAILED | "
                        f"OrgId={org['OrgId']} | "
                        f"OrgName={org['Name']} | "
                        f"ProjectId={project['ProjectId']} | "
                        f"ProcessId=N/A | "
                        f"Error={str(e)}"
                    )

            # ONE summary log per org after all projects processed
            logger.info(
                f"OrgId={org['OrgId']} | "
                f"OrgName={org['Name']} | "
                f"TotalProjects={len(projects)} | "
                f"TotalProcesses={total_processes}"
            )

    except pyodbc.Error as e:
        logger.error(f"Database error -> {e}")
        exit(1)

    finally:
        try:
            cursor.close()
            connection.close()
            logger.info("Database connection closed.")
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")


# ─────────────────────────────────────────────
#  REQUIRED FOR AZURE AUTOMATION / PROD
# ─────────────────────────────────────────────
if __name__ == "__main__":
    main()









    # ─────────────────────────────────────────────
#  FOR LOCAL TESTING ONLY
#  Reads keys from MongoDBKeys 1.txt
#  Format: Org-{name};Public-{key};Private-{key}
# ─────────────────────────────────────────────

FILE_PATH = "MongoDBKeys 1.txt"

def read_keys_from_file(file_path):
    keys_list = []
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                line = line.strip()

                # Skip empty lines
                if not line:
                    continue

                parts = line.split(";")

                if len(parts) != 3:
                    raise ValueError(
                        f"Invalid format at line {line_number}: {line}"
                    )

                org = normalize_org_name(
                    parts[0].replace("Org-", "", 1)
                )
                public_key = (
                    parts[1]
                    .replace("Public-", "")
                    .strip()
                )
                private_key = (
                    parts[2]
                    .replace("Private-", "")
                    .strip()
                )

                keys_list.append({
                    "org":         org,
                    "aliases":     build_org_aliases(org),
                    "public_key":  public_key,
                    "private_key": private_key,
                })

    except FileNotFoundError:
        logger.error(f"ERROR: File not found -> {file_path}")
        exit(1)
    except Exception as e:
        logger.error(f"ERROR while reading file: {e}")
        exit(1)

    return keys_list


def main():
    # ── GUARD: Check file exists ─────────────────
    if not FILE_PATH:
        logger.error("FILE_PATH is empty")
        return

    # ── Load keys from txt file ──────────────────
    auth_keys = read_keys_from_file(FILE_PATH)

    if not auth_keys:
        logger.error("No keys loaded from file")
        return

    # ... rest of main() stays same