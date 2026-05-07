Hey Charles! Yes the Auth Protocol matters.

We use HTTPDigestAuth — make sure you are 
importing and using it like this:

from requests.auth import HTTPDigestAuth

auth = HTTPDigestAuth(public_key, private_key)

response = requests.get(
    url,
    headers=headers,
    auth=auth,
    verify=False
)

Also make sure your headers are:
headers = {
    "Accept": "application/vnd.atlas.2025-03-12+json"
}

The MongoDB Atlas API requires Digest Auth 
specifically — Basic Auth will not work.
Let me know if that helps!