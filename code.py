def main():
    """
    MAIN ENTRY POINT
    This function is executed when the script runs in prod
    """

    FILE_PATH = "MongoDBKeys 1.txt"

    auth_keys = read_keys_from_file(FILE_PATH)

    # Build a lookup map: OrgId -> auth entry
    auth_lookup = {entry["org"]: entry for entry in auth_keys}

    connection = connect_to_db()
    cursor = connection.cursor()
    try:
        organizations = read_org_table(cursor)

        cluster_map = read_cluster_map(cursor)

        for org in organizations:
            total_processes = 0

            auth_entry = auth_lookup.get(normalize_org_name(org["Name"]))

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

            projects = read_project_table(cursor, org["OrgKey"])

            for project in projects:
                try:
                    processes = get_project_processes(
                        project["ProjectId"], auth_entry,
                        org["OrgKey"], project["ProjectKey"]
                    )
                    total_processes += len(processes)
                    write_processes_to_db(cursor, processes, cluster_map)
                    cursor.connection.commit()

                except Exception as e:
                    logger.error(
                        f"FAILED | "
                        f"OrgId={org['OrgId']} | "
                        f"OrgName={org['Name']} | "
                        f"ProjectId={project['ProjectId']} | "
                        f"ProcessId=N/A | "
                        f"Error={str(e)}"
                    )

            logger.info(
                f"OrgId={org['OrgId']} | "
                f"OrgName={org['Name']} | "
                f"TotalProjects={len(projects)} | "
                f"TotalProcesses={total_processes}"
            )

    except pyodbc.Error as e:
        print(f"ERROR reading table data -> {e}")
        exit(1)

    finally:
        cursor.close()
        connection.close()


# Call API using the loaded keys
#call_api(auth_keys)

# REQUIRED FOR PROD EXECUTION
if __name__ == "__main__":
    main()