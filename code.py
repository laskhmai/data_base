from requests.auth import HTTPDigestAuth
import requests

# Use your existing values
PROJECT_ID = "PASTE_PROJECT_ID"

# From the process output you already fetched
PROCESS_ID = "PASTE_HOSTNAME:PORT"

url = (
    f"https://cloud.mongodb.com/api/atlas/v2/groups/"
    f"{PROJECT_ID}/processes/{PROCESS_ID}/measurements"
)

headers = {
    "Accept": "application/vnd.atlas.2025-03-12+json"
}

params = {
    "granularity": "PT5M",
    "period": "PT1H"
}

response = requests.get(
    url,
    headers=headers,
    params=params,
    auth=HTTPDigestAuth(public_key, private_key),
    timeout=30
)

print("Status Code:", response.status_code)

if response.status_code == 200:
    data = response.json()

    print("Process:", data.get("processId"))
    print("Start:", data.get("start"))
    print("End:", data.get("end"))
    print("Granularity:", data.get("granularity"))

    measurements = data.get("measurements", [])

    print("Measurements found:", len(measurements))

    for metric in measurements:
        print("\n----------------------------")
        print("Metric:", metric.get("name"))
        print("Units:", metric.get("units"))

        points = metric.get("dataPoints", [])

        for point in points[:5]:
            print(
                point.get("timestamp"),
                "=>",
                point.get("value")
            )

else:
    print("Metrics API failed")
    print(response.text[:1500])