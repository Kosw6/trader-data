import requests

def get_cik_from_ticker(ticker):
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    data = response.json()
    
    for company in data.values():
        if company["ticker"].lower() == ticker.lower():
            return str(company["cik_str"]).zfill(10)  # 10자리 CIK 형식
    return None

# 사용 예시
ticker = "AAPL"
cik = get_cik_from_ticker(ticker)

if cik:
    print(f"{ticker}의 CIK는 {cik}입니다.")
else:
    print("CIK를 찾을 수 없습니다.")
