import requests
import json
from dotenv import load_dotenv
import os

def get_accesstoken() : 
    load_dotenv()
    appkey = os.getenv('APPKEY')
    appsecret = os.getenv('APPSECRET')
    url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
    header = {
        "Content-Type": "application/json; charset=utf-8"
    }
    payload = {
        "grant_type":"client_credentials",
        "appkey":appkey,
        "appsecret":appsecret
    }
    try:
        response = requests.post(url,headers=header,json=payload)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Error: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print("Request failed:", e)