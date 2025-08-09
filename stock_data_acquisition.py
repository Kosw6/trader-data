import requests
import json
import oauth_stock
import os_env
from dotenv import load_dotenv, find_dotenv
import os
import mongo_crud
from datetime import datetime, timedelta
import time
from bson import Int64

url = "https://openapi.koreainvestment.com:9443/uapi/overseas-price/v1/quotations/dailyprice"

dotenv_file = find_dotenv()
load_dotenv(dotenv_file, override=True)
appkey = os.getenv('APPKEY')
appsecret = os.getenv('APPSECRET')



accesstoken = "Bearer "+os_env.get_accessToken()


def get_recent_stock_ts_data(excd,symb):
    stock_recent_date = mongo_crud.find_stock_ts_recent(symb)["timestamp"]
    #이미 최신화가 되었을 경우
    if stock_recent_date.date() == datetime.now().date() :
        return
    new_list = []
    headers = {
    "Content-Type": "application/json; charset=utf-8",
    "authorization":accesstoken,
    "tr_id":"HHDFS76240000",
    "appkey": appkey,
    "appsecret":appsecret,
    "tr_cont":"N"
    }

    payload = {
    "AUTH": "",
    "EXCD": excd,
    "SYMB": symb,
    "GUBN": "0",
    "BYMD": "",
    "MODP": "0"
    }
    try:
        response = requests.get(url,headers=headers,params=payload)
        if response.status_code == 200:
              while True:
                for item in response.json()['output2']:
                    timestamp = datetime.strptime(item["xymd"], "%Y%m%d")
                    if(timestamp<=stock_recent_date):
                            mongo_crud.save_stock_ts_data(new_list)
                            # print(new_list)
                            # print(len(new_list))
                            print(symb+"저장완료")
                            return
                    new_obj = {
                    "symb": symb,
                    "timestamp": timestamp,
                    "close":float(item["clos"]),
                    "open":float(item["open"]),
                    "high":float(item["high"]),
                    "low":float(item["low"]),
                    "volume":Int64(item["tvol"])
                    }
                    new_list.append(new_obj)
                #만약 마지막 응답이면
                if response.headers["tr_cont"] in ["D", "E"] :
                    break
                new_date = datetime.strptime(new_list[-1]["xymd"], "%Y%m%d")-timedelta(days=1)
                new_date = new_date.strftime("%Y%m%d")
                payload["BYMD"] = new_date
                response = requests.get(url,headers=headers,params=payload)
    except Exception as e:
        print(response)
        print("Request failed:", e)

def get_total_recent_stock_ts_data():

    stock_list = mongo_crud.find_stock_list()
    stock_data_list = mongo_crud.find_stock_data_ts_list()
    # print(len(stock_list))
    print(len(stock_data_list))
    #기업 리스트에서 실제 데이터가 들어가지 않은 리스트를 뽑는 과정
    sub_list = list(filter(lambda x:x['symb'] in stock_data_list,stock_list))
    print(len(sub_list))
    # print(sub_list)
    for item in sub_list:
        print(item["excd"],item["symb"])
        get_recent_stock_ts_data(item["excd"],item["symb"])
        time.sleep(0.02)
# def get_stock_missing_data():
    
#해야 될 것
#자동화, 기업 리스트업해서 자동화 하는 코드 작성
#최신 데이터를 가져오는 코드 작성, -> 데이터셋에 가장 최신의 날짜를 가져오고 다음 날짜부터 가져오기
get_total_recent_stock_ts_data()