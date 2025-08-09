import requests
import json
import pandas as pd

# BLS API 정보
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
API_KEY = "5d3925abd2704b209edee384dc287688"  # 본인의 BLS Registration Key 입력

START_YEAR = 2022
END_YEAR = 2025
SERIES_ID = "CUUR0000SA0"

def fetch_cpi_data(start_year, end_year, api_key):
    headers = {"Content-Type": "application/json"}
    data = json.dumps({
        "seriesid": [SERIES_ID],
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": api_key
    })
    response = requests.post(BLS_API_URL, headers=headers, data=data)
    response.raise_for_status()
    result = response.json()
    if result.get("status") != "REQUEST_SUCCEEDED":
        raise Exception("BLS API 요청 실패: " + str(result))
    records = []
    for series in result["Results"]["series"]:
        for item in series["data"]:
            if item["period"].startswith("M"):
                records.append({
                    "year": int(item["year"]),
                    "month": int(item["period"][1:]),
                    "cpi": float(item["value"])
                })
    df = pd.DataFrame(records)
    # 연-월 컬럼 추가
    df["date"] = pd.to_datetime(df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2))
    # 날짜 기준 오름차순 정렬
    df = df.sort_values("date").reset_index(drop=True)
    return df

if __name__ == "__main__":
    cpi_df = fetch_cpi_data(START_YEAR, END_YEAR, API_KEY)
    # 전체 데이터프레임 출력
    print(cpi_df[["date", "cpi"]])
