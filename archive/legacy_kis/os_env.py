import os
import oauth_stock
from dotenv import load_dotenv, find_dotenv, set_key
from datetime import datetime

def save_accessToken(access_token,access_token_token_expired) :
    dotenv_file = find_dotenv()
    load_dotenv(dotenv_file, override=True)
    set_key(dotenv_file, "ACCESS_TOKEN", access_token)
    set_key(dotenv_file, "ACCESS_TOKEN_TOKEN_EXPIRED", access_token_token_expired)

def get_accessToken() :
    dotenv_file = find_dotenv()
    load_dotenv(dotenv_file, override=True)
    access_token = os.getenv('ACCESS_TOKEN')
    access_token_token_expired = os.getenv('ACCESS_TOKEN_TOKEN_EXPIRED')
    # 토큰 만료되었을때
    print(datetime.now())
    print(datetime.strptime(access_token_token_expired,"%Y-%m-%d %H:%M:%S"))
    print(datetime.now() < datetime.strptime(access_token_token_expired,"%Y-%m-%d %H:%M:%S"))
    if datetime.now() < datetime.strptime(access_token_token_expired,"%Y-%m-%d %H:%M:%S"):
        print("success")
        return access_token
    else:
        access_response = oauth_stock.get_accesstoken()
        access_token=access_response["access_token"]
        access_token_token_expired=access_response["access_token_token_expired"]
        save_accessToken(access_token,access_token_token_expired)
        return access_token
    
def get_bls_api_key() :
    dotenv_file = find_dotenv()
    load_dotenv(dotenv_file, override=True)
    bls_api_key = os.getenv('BLS_API_KEY')
    
    return bls_api_key
