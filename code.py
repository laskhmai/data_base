# ------------------------------------------------------------
# Fetch MongoDB processes for one project
# ------------------------------------------------------------

PROJECT_ID = "PASTE_ONE_PROJECT_ID_HERE"

process_url = (
    f"https://cloud.mongodb.com/api/atlas/v2/groups/"
    f"{PROJECT_ID}/processes"
)

headers = {
    "Accept": "application/vnd.atlas.2025-03-12+json"
}

response = requests.get(
    process_url,
    headers=headers,
    auth=HTTPDigestAuth(public_key, private_key),
    timeout=30
)

print("Status Code:", response.status_code)

if response.status_code == 200:
    data = response.json()
    processes = data.get("results", [])

    print("Processes found:", len(processes))

    for p in processes:
        print("--------------------------------")
        print("ID:", p.get("id"))
        print("Host:", p.get("hostname"))
        print("Port:", p.get("port"))
        print("Type:", p.get("typeName"))
        print("Replica Set:", p.get("replicaSetName"))
        print("Version:", p.get("version"))
        print("Last Ping:", p.get("lastPing"))

else:
    print("Process API call failed")
    print(response.text[:1000])