import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("BLS_API_KEY")
# BLS API URL (예: 버전 2)
url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

payload = {
    "seriesid": ["WPUFD4"],  # PPI Final Demand (Unadjusted)
    "startyear": "2025",
    "endyear": "2025",
    "registrationkey": api_key
}

headers = {'Content-type': 'application/json'}

response = requests.post(url, data=json.dumps(payload), headers=headers)
data = response.json()

print(json.dumps(data, indent=2, ensure_ascii=False))
