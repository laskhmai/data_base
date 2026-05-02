# Key Vault secret base names for each MongoDB organization.
KEY_LIST = [
    "mongo-caad", "mongo-centerwell-patient-mastering", "mongo-cgx", "mongo-cloudea",
    "mongo-cloudmlp", "mongo-consumerhub", "mongo-core-cognitive-api---data",
    "mongo-corporate-it", "mongo-database-services", "mongo-dige",
    "mongo-enrollmentsystems", "mongo-enterprise-information-protection",
    "mongo-enterprise-platforms---analytics", "mongo-fhir",
    "mongo-healthcare-interoperability", "mongo-homegrid",
    "mongo-medc-claims-adjudication", "mongo-pharmacy-benefits-management",
    "mongo-pharmacy-fulfillment", "mongo-provider", "mongo-retail-medicare-provider",
    "mongo-shared001", "mongo-softwaredevelopmenttools",
    "mongo-voice-technology---process-innovation", "mongo-wellness---rewards"
]


def build_org_aliases(secret_base_name):
   """
    Returns normalized alias candidates for org-name matching.
    """
    normalized = normalize_org_name(secret_base_name)
    aliases = {normalized}
    if normalized.startswith("mongo-"):
        aliases.add(normalized.replace("mongo-", "", 1))
    return aliases




def read_keys_from_vault(key_list):
    """
    Reads MongoDB Atlas public/private API keys from Key Vault.
    Each entry in key_list is a secret base name like 'mongo-caad'.
    """
    keys_list = []
    for key_name in key_list:
        try:
            public_secret_name = f"{key_name}-public-key"
            private_secret_name = f"{key_name}-private-key"

            public_key = keyvault.fetch_secret(public_secret_name)
            private_key = keyvault.fetch_secret(private_secret_name)

            if not public_key or not private_key:
                logger.warning(
                    f"Missing Key Vault secrets for {key_name}; expected "
                    f"{public_secret_name} and {private_secret_name}."
                )
                continue
            keys_list.append(
                {
                    "org": normalize_org_name(key_name),
                    "aliases": build_org_aliases(key_name),
                    "public_key": public_key,
                    "private_key": private_key,
                }
            )
        except Exception as e:
            logger.error(f"Failed to load Key Vault secrets for {key_name}: {e}")



            auth_keys = read_keys_from_vault(KEY_LIST)



if not auth_keys:
        logger.error("No MongoDB Atlas auth keys were loaded from Key Vault.")
        return



 f"Org={item['org']}"



auth_lookup = {}
    for entry in auth_keys:
        for alias in entry.get("aliases", {entry["org"]}):
            auth_lookup[alias] = entry