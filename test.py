from dotenv import load_dotenv
import os
import os_env
from datetime import datetime

# .env 파일 로드
# load_dotenv()

# 환경 변수 가져오기
# access_token = os.getenv('ACCESS_TOKEN')
# access_token_token_expired = os.getenv('ACCESS_TOKEN_TOKEN_EXPIRED')
# if access_token_token_expired and datetime.now() < datetime.strptime(access_token_token_expired,"%Y-%m-%d %H:%M:%S"):
#     print("ok")
now = datetime.now()
# expire = datetime.strptime(access_token_token_expired,"%Y-%m-%d %H:%M:%S")
print("now is : "+str(now.date()))
# print(expire)
# print(now > expire)
# print(now < expire)
# print(f"ACCESS_TOKEN: {access_token}")
# print(f"ACCESS_TOKEN_TOKEN_EXPIRED: {access_token_token_expired}")
# result=os_env.get_accessToken()
# print(result)