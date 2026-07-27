import requests
import json
import oauth_stock
import os_env
from dotenv import load_dotenv, find_dotenv
import os
import mongo_crud

url = "https://openapi.koreainvestment.com:9443/uapi/overseas-price/v1/quotations/inquire-search"

dotenv_file = find_dotenv()
load_dotenv(dotenv_file, override=True)
appkey = os.getenv('APPKEY')
appsecret = os.getenv('APPSECRET')


accesstoken = "Bearer "+os_env.get_accessToken()
headers = {
    "Content-Type": "application/json; charset=utf-8",
    "authorization":accesstoken,
    "tr_id":"HHDFS76410000",
    "appkey": appkey,
    "appsecret":appsecret,
    "tr_cont":"3",
    "KEYB":""
}

payload = {
    "AUTH": "",
    "EXCD": "NYS",
    "CO_ST_VALX":"4,800,000"
}

try:
    response = requests.get(url,headers=headers,params=payload)
    if response.status_code == 200:
        new_list = []
        print(response.json())
        for item in response.json()['output2']:
            new_obj = {
                "excd": item["excd"],
                "symb": item["symb"],
                "name": item["name"],
                "ename": item["ename"]
            }
            new_list.append(new_obj)
        # print(new_list)
        # mongo_crud.save_stock_list(new_list)
        # ++
        # 몽고디비에서 스톡 리스트 컬렉션에서 기업명만 가져와서 없는 기업만 추가하게끔 하는 코드
    else:
        print(f"Error: {response.status_code}, {response.text}")
except requests.exceptions.RequestException as e:
    print("Request failed:", e)
