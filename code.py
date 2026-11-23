import requests
from requests.auth import HTTPDigestAuth

# CAAD MongoDB Organization ID
ORG_ID = "64d6a2d52bacd4253e544f49"

# Vault location you showed earlier
MONGO_SECRET_PATH = "cldcostop.appsvc114662/MongoDB"
VAULT_MOUNT = "secret"   # keep the same mount used in your working PostgreSQL code

# 1. Read MongoDB API keys from Vault
secret_response = vault.secrets.kv.v2.read_secret_version(
    path=MONGO_SECRET_PATH,
    mount_point=VAULT_MOUNT
)

mongo_secrets = secret_response["data"]["data"]

public_key = mongo_secrets["caad_public_key"]
private_key = mongo_secrets["caad_private_key"]

print("MongoDB keys retrieved from Vault successfully")
# DO NOT print the actual keys


# 2. Fetch projects for CAAD organization
url = f"https://cloud.mongodb.com/api/atlas/v2/orgs/{ORG_ID}/groups"

headers = {
    "Accept": "application/vnd.atlas.2025-03-12+json"
}

response = requests.get(
    url,
    headers=headers,
    auth=HTTPDigestAuth(public_key, private_key),
    timeout=30
)

print("Status Code:", response.status_code)


# 3. Display project names and IDs only
if response.status_code == 200:
    data = response.json()

    projects = data.get("results", [])

    print("Projects found:", len(projects))

    for project in projects:
        print(
            "Project Name:", project.get("name"),
            "| Project ID:", project.get("id")
        )

else:
    print("MongoDB API call failed")
    print(response.text)