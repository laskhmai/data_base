auth_keys = read_keys_from_file(FILE_PATH)
 
    # ── GUARD: Check keys loaded ─────────────────
    if not auth_keys:
        logger.error(
            "No keys loaded from file. "
            "Check MongoDBKeys 1.txt exists and "
            "follows format: "
            "Org-{name};Public-{key};Private-{key}"
        )
        return
 
    # ── Build org lookup map ─────────────────────
    auth_lookup = {}
    for entry in auth_keys:
        for alias in entry.get("aliases", {entry["org"]}):
            auth_lookup[alias]                     = entry
            auth_lookup[canonical_org_name(alias)] = entry
 
    # ── Connect to database ──────────────────────
    connection = connect_to_db()
    cursor     = connection.cursor()
 
    try:
        organizations = read_org_table(cursor)
        cluster_map   = read_cluster_map(cursor)
 
        for org in organizations:
 
            # Reset counter for each org
            total_processes = 0
 
            # Match org to auth entry
            normalized_name = normalize_org_name(org["Name"])
            canonical_name  = canonical_org_name(org["Name"])
            auth_entry = (
                auth_lookup.get(normalized_name)
                or auth_lookup.get(canonical_name)
            )
 
            # No auth found — log and skip
            if not auth_entry:
                logger.error(
                    f"FAILED | "
                    f"OrgId={org['OrgId']} | "
                    f"OrgName={org['Name']} | "
                    f"ProjectId=N/A | "
                    f"ProcessId=N/A | "
                    f"Reason=No auth entry found in txt file"
                )
                continue
 
            # Get all projects for this org
            projects = read_project_table(
                cursor, org["OrgKey"]
            )
 
            for project in projects:
                try:
                    # Fetch processes from Atlas API
                    processes = get_project_processes(
                        project["ProjectId"],
                        auth_entry,
                        org["OrgKey"],
                        project["ProjectKey"],
                    )
 
                    # Accumulate total
                    total_processes += len(processes)
 
                    # Write to database
                    write_processes_to_db(
                        cursor, processes, cluster_map
                    )
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
 
            # ONE summary log per org
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
#  ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    main()
 