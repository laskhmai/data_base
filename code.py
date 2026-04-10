def find_cluster_key(user_alias, project_key, clusters):
    """
    Match process to cluster using userAlias
    against ConnectionStrings
    
    Example:
    userAlias = "coreapi-accums-prod-shard-00-00.rwjvw.mongodb.net"
    Extract  = "coreapi-accums-prod"
    Match in ConnectionStrings of same project
    """
    if not user_alias:
        return None

    # Extract cluster prefix from userAlias
    # "coreapi-accums-prod-shard-00-00.rwjvw.mongodb.net"
    # becomes "coreapi-accums-prod"
    cluster_prefix = user_alias.split("-shard")[0]

    # Find matching cluster in same project
    for cluster in clusters:
        if cluster["ProjectKey"] == project_key:
            conn_str = cluster["ConnectionStrings"] or ""
            if cluster_prefix in conn_str:
                return cluster["ClusterKey"]

    return None  # No match found



def read_cluster_map(cursor):
    """
    Now returns ALL clusters with their
    connection strings for proper matching
    """
    query = """
        SELECT 
            ClusterKey,
            ProjectKey,
            Name,
            ConnectionStrings
        FROM [AtlasMongoDB].[Clusters]
        WHERE IsDeleted = 0
    """
    cursor.execute(query)
    clusters = []
    for row in cursor.fetchall():
        clusters.append({
            "ClusterKey": row[0],
            "ProjectKey": row[1],
            "Name": row[2],
            "ConnectionStrings": row[3]
        })
    return clusters



cluster_key = find_cluster_key(
            p.get("userAlias"),      # ← use userAlias
            p["ProjectKey"],          # ← same project only
            clusters                  # ← all clusters
        )


 NEW - get full cluster list
    clusters = read_cluster_map(cursor)
    logger.info(f"Clusters loaded: {len(clusters)}")

SELECT 
    ProjectKey,
    ClusterKey,
    COUNT(*) as ProcessCount
FROM [AtlasMongoDB].[Process]
WHERE IsDeleted = 0
GROUP BY ProjectKey, ClusterKey
ORDER BY ProjectKey



 # Handle ALL types
    if "-shard" in user_alias:
        cluster_prefix = user_alias.split("-shard")[0]
    elif "-config" in user_alias:
        cluster_prefix = user_alias.split("-config")[0]
    elif "-mongos" in user_alias:
        cluster_prefix = user_alias.split("-mongos")[0]
    else:
        cluster_prefix = user_alias.split(".")[0]
