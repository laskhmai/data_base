[12:03 PM, 5/2/2026] Manish Anna @ Ind Ranga: 
import re

[12:04 PM, 5/2/2026] Manish Anna @ Ind Ranga: 
def canonical_org_name(org_name):
    """
    Builds a canonical token for resilient org matching across systems.
    Examples: 'mongo-caad' -> 'caad', 'CAAD' -> 'caad'.
    """
    normalized = normalize_org_name(org_name)
    if normalized.startswith("mongo-"):
        normalized = normalized.replace("mongo-", "", 1)
    return re.sub(r"[^a-z0-9]", "", normalized)

[12:06 PM, 5/2/2026] Manish Anna @ Ind Ranga: 
    aliases.add(canonical_org_name(secret_base_name))
    aliases.add(canonical_org_name(normalized.replace("mongo-", "", 1)))



auth_lookup[canonical_org_name(alias)] = entry



            normalized_name = normalize_org_name(org["Name"])
            canonical_name = canonical_org_name(org["Name"])
            auth_entry = auth_lookup.get(normalized_name) or auth_lookup.get(canonical_name)