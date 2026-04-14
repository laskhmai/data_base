def write_processes_to_db(cursor, processes, clusters):
    """
    Upserts process records into [MongoDB].[Process].
    Uses explicit CHECK + UPDATE/INSERT for Synapse compatibility.
    """
    if not processes:
        return

    audit_utc = datetime.now(timezone.utc)
    verified_utc = datetime.now(timezone.utc)

    check_query = """
        SELECT COUNT(*) FROM [MongoDB].[Process]
        WHERE ProcessId = ? AND ProjectKey = ?
    """

    update_query = """
        UPDATE [MongoDB].[Process]
        SET OrgKey=?, ClusterKey=?, Name=?, ReplicaSetName=?,
            ProcessType=?, Links=?, UserAlias=?, version=?,
            ProcessUpdatedDate=?, VerifiedUtc=?, AuditUtc=?,
            AuditUser=?, IsDeleted=?
        WHERE ProcessId=? AND ProjectKey=?
    """

    insert_query = """
        INSERT INTO [MongoDB].[Process]
            (OrgKey, ProjectKey, ClusterKey, Name, ReplicaSetName,
             ProcessId, ProcessType, Links, UserAlias, version,
             ProcessCreatedDate, ProcessUpdatedDate, VerifiedUtc,
             AuditUtc, AuditUser, IsDeleted)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    for p in processes:
        cluster_key = find_cluster_key(
            p.get("userAlias"), p.get("ProjectKey"), clusters
        )
        process_id  = p.get("id")
        project_key = p.get("ProjectKey")
        links       = json.dumps(p.get("links")) if p.get("links") is not None else None

        cursor.execute(check_query, (process_id, project_key))
        exists = cursor.fetchone()[0]

        if exists:
            cursor.execute(update_query, (
                p.get("OrgKey"), cluster_key,
                p.get("hostname"), p.get("replicaSetName"),
                p.get("typeName"), links, p.get("userAlias"), p.get("version"),
                p.get("lastPing"), verified_utc, audit_utc,
                "Lenticular", 0,
                process_id, project_key
            ))
        else:
            cursor.execute(insert_query, (
                p.get("OrgKey"), project_key, cluster_key,
                p.get("hostname"), p.get("replicaSetName"),
                process_id, p.get("typeName"), links, p.get("userAlias"), p.get("version"),
                p.get("created"), p.get("lastPing"), verified_utc,
                audit_utc, "Lenticular", 0
            ))

    logger.info(f"{len(processes)} process record(s) upserted into [MongoDB].[Process].")