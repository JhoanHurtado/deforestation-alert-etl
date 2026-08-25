import requests

BASE_URL = "https://data-api.globalforestwatch.org"

token_payload = {
    "username": "jhoanezequielh@gmail.com",
    "password": "nordEn-3foxco-zixhat"
}

response = requests.post(
    f"{BASE_URL}/auth/token",
    data=token_payload,  # ojo: es form-urlencoded, no json
)
token_data = response.json()
access_token = token_data["data"]["access_token"]

headers = {
    "Authorization": f"Bearer {access_token}"
}

apikey_payload = {
    "alias": "uao-etl-deforestacion-bolivia",
    "organization": "Universidad Autonoma de Occidente",
    "email": "jhoanezequielh@gmail.com",
    "domains": [],  # vacío = key sin restricción de dominio (sirve para scripts/notebooks)
    "never_expires": False
}

response = requests.post(f"{BASE_URL}/auth/apikey", json=apikey_payload, headers=headers)
apikey_data = response.json()

if apikey_data.get("status") == "failed":
    import re, time
    # Retry with a unique alias
    apikey_payload["alias"] = f"uao-etl-deforestacion-bolivia-{int(time.time())}"
    response = requests.post(f"{BASE_URL}/auth/apikey", json=apikey_payload, headers=headers)
    apikey_data = response.json()
    if apikey_data.get("status") == "failed":
        raise RuntimeError(f"Could not create API key: {apikey_data['message']}")
    api_key = apikey_data["data"]["api_key"]
else:
    api_key = apikey_data["data"]["api_key"]

print("Tu API key:", api_key)

with open(".env", "w") as f:
    f.write(f"GFW_API_KEY={api_key}\n")