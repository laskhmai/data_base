ERIFY_SSL = False

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

def read_cluster_map(cursor):
    """
    Reads [AtlasMongoDB].[Clusters] and returns a dict mapping
    ProjectKey -> ClusterKey for use as a foreign key in Processor.
    """
    query = """
        SELECT [ProjectKey], [ClusterKey]
        FROM [AtlasMongoDB].[Clusters]
        WHERE IsDeleted = 0;
    """
    cursor.execute(query)
    return {row[0]: row[1] for row in cursor.fetchall()}

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
    auth = HTTPDigestAuth(
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
        "Version",
    )

    try:
        response = requests.get(url, headers=headers, auth=auth, timeout=10, verify=VERIFY_SSL)
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


def write_processes_to_db(cursor, processes, cluster_map):
    """
    Upserts process records into [AtlasMongoDB].[Process] using MERGE.
    Unique key: (ProcessId, ProjectKey).
    - Inserts the record if it does not exist.
    - Updates mutable fields if it already exists (ProcessCreatedDate is preserved).
    ClusterKey is resolved from cluster_map using ProjectKey.
    """
    if not processes:
        return

    merge_query = """
        MERGE [AtlasMongoDB].[Process] AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?))
            AS source (OrgKey, ProjectKey, ClusterKey, Name, ReplicaSetName,
                       ProcessId, ProcessType, Links, UserAlias, Version,
                       ProcessCreatedDate, ProcessUpdatedDate, VerifiedUtc,
                       AuditUtc, AuditUser, IsDeleted)
        ON target.ProcessId = source.ProcessId
           AND target.ProjectKey = source.ProjectKey
        WHEN MATCHED THEN
            UPDATE SET
                OrgKey             = source.OrgKey,
                ClusterKey         = source.ClusterKey,
                Name               = source.Name,
                ReplicaSetName     = source.ReplicaSetName,
                ProcessType        = source.ProcessType,
                Links              = source.Links,
                UserAlias          = source.UserAlias,
                Version            = source.Version,
                ProcessUpdatedDate = source.ProcessUpdatedDate,
                VerifiedUtc        = source.VerifiedUtc,
                AuditUtc           = source.AuditUtc,
                AuditUser          = source.AuditUser,
                IsDeleted          = source.IsDeleted
        WHEN NOT MATCHED THEN
            INSERT (OrgKey, ProjectKey, ClusterKey, Name, ReplicaSetName,
                    ProcessId, ProcessType, Links, UserAlias, Version,
                    ProcessCreatedDate, ProcessUpdatedDate, VerifiedUtc,
                    AuditUtc, AuditUser, IsDeleted)
            VALUES (source.OrgKey, source.ProjectKey, source.ClusterKey,
                    source.Name, source.ReplicaSetName, source.ProcessId,
                    source.ProcessType, source.Links, source.UserAlias,
                    source.Version, source.ProcessCreatedDate,
                    source.ProcessUpdatedDate, source.VerifiedUtc,
                    source.AuditUtc, source.AuditUser, source.IsDeleted);
    """

    audit_utc = datetime.now(timezone.utc)
    verified_utc = datetime.now(timezone.utc)

    rows = [
        (
            p["OrgKey"],
            p["ProjectKey"],
            cluster_map.get(p["ProjectKey"]),              # ClusterKey
            p.get("hostname"),                             # Name
            p.get("replicaSetName"),                       # ReplicaSetName
            p.get("id"),                                   # ProcessId
            p.get("typeName"),                             # ProcessType
            json.dumps(p.get("links")) if p.get("links") is not None else None,  # Links
            p.get("userAlias"),                            # UserAlias
            p.get("version"),                              # Version
            p.get("created"),                              # ProcessCreatedDate
            p.get("lastPing"),                             # ProcessUpdatedDate
            verified_utc,                                  # VerifiedUtc
            audit_utc,                                     # AuditUtc
            "Lenticular",                                  # AuditUser
            0,                                             # IsDeleted
        )
        for p in processes
    ]

    for row in rows:
        cursor.execute(merge_query, row)
    logger.info(f"{len(rows)} process record(s) upserted into [AtlasMongoDB].[Process].")


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

        cluster_map = read_cluster_map(cursor)
        logger.info(f"Cluster map loaded: {len(cluster_map)} entries")

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

            auth_entry = auth_lookup.get(org["Name"])

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
                write_processes_to_db(cursor, processes, cluster_map)
                cursor.connection.commit()

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



    As a data engineer, I need to collect MongoDB Atlas 
process inventory from all organizations and projects 
every 6 hours so that we can map processes to clusters 
and collect metrics for right-sizing recommendations.

1. Read all Organizations from AtlasMongoDB.Organization table
2. For each Org, read Projects from AtlasMongoDB.Projects table
3. For each Project, call MongoDB Atlas API to get processes
4. Map each process to its cluster using ProjectKey
5. Store process info in AtlasMongoDB.Processor table
6. Handle duplicate processes (upsert logic - update if exists)
7. Schedule to run every 6 hours
8. Log all errors and successes


ork completed so far:

1. Reviewed the MongoDB Atlas Metrics documentation 
   shared by Neeraja

2. Tested the MongoDB Atlas API in Postman successfully
   - Called /processes endpoint - Status 200 OK
   - Called /measurements endpoint for CPU metrics 
   - Status 200 OK

3. Understood the full architecture:
   - Org → Projects → Clusters → Processes
   - Processes must be mapped to clusters using 
     ProjectKey and replicaSetName

4. Code written for process inventory collection:
   - Reads Orgs and Projects from database
   - Calls MongoDB Atlas API per project
   - Maps processes to clusters using ProjectKey
   - Stores results in AtlasMongoDB.Processor table


i Khwaja, for creating ADLS in Cloud 3.0, you'll need to create a regular Azure storage account first, then attach it to the data lake module. Here's the only template we have for this: https://github.com/Corp-Func-and-Ent-Sys-EMU/se-azure-DataLakeAnalytics-legacy-template-tfc — it hasn't been updated since 2023 but should work with Cloud 3 and current provider versions.
    
    ou can keep both in the same Terraform stack. Just add a depends_on clause pointing to your storage account resource inside the datalake module block. That way Terraform automatically creates the storage first before provisioning the data lake. 