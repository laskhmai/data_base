# Function to connect to the database
def connect_to_db():
    try:
        logger.info("Attempting to connect to the database...")
        conn = pyodbc.connect(
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']};",
            fast_executemany=True
        )
        logger.info("Successfully connected to the database.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        exit(1)

def read_org_table(cursor):
    """
    Reads Org table data from SQL Server
    and returns it as a list of dictionaries
    """

    query = """
        SELECT
            OrgKey,
            OrgId,
            Name,
            Hash,
            Links,
            IsDeleted,
            Verified_UTC,
            Audit_User,
            Audit_UTC
        FROM AtlasMongoDB.Organization
        WHERE IsDeleted = 0;
    """

    org_list = []
    cursor.execute(query)

    columns = [column[0] for column in cursor.description]

    for row in cursor.fetchall():
        org_data = dict(zip(columns, row))
        org_list.append(org_data)
    return org_list

def read_project_table(cursor, orgId):
    """
    Reads Org table data from SQL Server
    and returns it as a list of dictionaries
    """

    query = """
        SELECT
            [ProjectKey]
            ,[ProjectId]
            ,[OrgKey]
            ,[Name]
            FROM [AtlasMongoDB].[Projects]
            WHERE IsDeleted = 0
            and OrgKey=?;
    """

    org_list = []
    cursor.execute(query, orgId)

    columns = [column[0] for column in cursor.description]

    for row in cursor.fetchall():
        org_data = dict(zip(columns, row))
        org_list.append(org_data)
    return org_list

def read_keys_from_file(file_path):
    """
    Reads authorization keys from a text file.

    Expected file format (one entry per line):
    Org-ABC ;Public-abc;Private-abc
    """

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

                org = parts[0].replace("Org-", "").strip()
                public_key = (
                    parts[1]
                    .replace("Public-", "")
                    .replace("public-", "")
                    .strip()
                )
                private_key = (
                    parts[2]
                    .replace("Private-", "")
                    .replace("private-", "")
                    .strip()
                )

                keys_list.append(
                    {
                        "org": org,
                        "public_key": public_key,
                        "private_key": private_key,
                    }
                )

    except FileNotFoundError:
        print(f"ERROR: File not found -> {file_path}")
        exit(1)

    except Exception as e:
        print(f"ERROR while reading file: {e}")
        exit(1)

    return keys_list

def get_project_processes(project_id, auth_entry, org_key, project_key):
    """
    Calls the MongoDB Atlas API to retrieve processes running against a project.
    Uses HTTP Digest authentication with the org's public/private keys.
    Returns a list of dicts with selected process fields for DB storage.
    OrgKey and ProjectKey are injected as foreign keys into each record.
    """
    url = f"https://cloud.mongodb.com/api/atlas/v2/groups/{project_id}/processes"
    headers = {
        "Accept": "application/vnd.atlas.2025-03-12+json",
    }
    auth = requests.auth.HTTPDigestAuth(
        auth_entry["public_key"],
        auth_entry["private_key"],
    )

    FIELDS = (
        "created",
        "hostname",
        "id",
        "lastPing",
        "links",
        "replicaSetName",
        "shardName",
        "typeName",
        "userAlias",
    )

    try:
        response = requests.get(url, headers=headers, auth=auth, timeout=10)
        response.raise_for_status()
        data = response.json()

        processes = [
            {"OrgKey": org_key, "ProjectKey": project_key,
             **{field: result.get(field) for field in FIELDS}}
            for result in data.get("results", [])
        ]
        logger.info(
            f"ProjectId={project_id} | Processes fetched: {len(processes)}"
        )
        return processes

    except requests.RequestException as e:
        logger.error(f"API call failed for ProjectId={project_id}: {e}")
        return []


def call_api(auth_entries):
    """
    Example API invocation using authorization headers
    """

    API_URL = "https://api.example.com/data"

    for entry in auth_entries:
        headers = {
            "X-ORG": entry["org"],
            "X-PUBLIC-KEY": entry["public_key"],
            "X-PRIVATE-KEY": entry["private_key"],
            "Content-Type": "application/json",
        }

        try:
            response = requests.get(API_URL, headers=headers, timeout=10)
            print(
                f"Org={entry['org']} | Status={response.status_code}"
            )

        except requests.RequestException as e:
            print(
                f"API call failed for Org={entry['org']} : {e}"
            )

def main():
    """
    MAIN ENTRY POINT
    This function is executed when the script runs in prod
    """

    FILE_PATH = "MongoDBKeys 1.txt"

    auth_keys = read_keys_from_file(FILE_PATH)

    print("\nAuthorization keys loaded successfully\n")

    for item in auth_keys:
        print(
            f"Org={item['org']} | PublicKey={item['public_key']}"
        )

    # Build a lookup map: OrgId -> auth entry
    auth_lookup = {entry["org"]: entry for entry in auth_keys}

    connection = connect_to_db()
    cursor = connection.cursor()
    try:
        organizations = read_org_table(cursor)

        print("\nOrganizations Loaded Successfully:\n")

        for org in organizations:
            print(f"OrgKey      : {org['OrgKey']}")
            print(f"OrgId       : {org['OrgId']}")
            print(f"Name        : {org['Name']}")
            print(f"Hash        : {org['Hash']}")
            print(f"Links       : {org['Links']}")
            print(f"IsDeleted   : {org['IsDeleted']}")
            print(f"Verified_UTC: {org['Verified_UTC']}")
            print(f"Audit_User  : {org['Audit_User']}")
            print(f"Audit_UTC   : {org['Audit_UTC']}")
            print("-" * 50)

            auth_entry = auth_lookup.get(org["OrgId"])

            if not auth_entry:
                logger.warning(
                    f"No auth entry found for OrgId={org['OrgId']} "
                    f"— skipping project and process fetch."
                )
                continue

            projects = read_project_table(cursor, org['OrgKey'])
            for project in projects:
                print(f"ProjectKey  : {project['ProjectKey']}")
                print(f"ProjectId   : {project['ProjectId']}")
                print(f"OrgKey      : {project['OrgKey']}")
                print(f"Name        : {project['Name']}")
                print("-" * 50)

                processes = get_project_processes(
                    project["ProjectId"], auth_entry,
                    org["OrgKey"], project["ProjectKey"]
                )
                print(f"  Processes fetched: {len(processes)}")
                # TODO: store `processes` list into DB (sssm)

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