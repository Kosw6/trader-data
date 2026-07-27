import time

from collectors.common import build_http_session


SEC_DATA_URL = "https://data.sec.gov"
SEC_WWW_URL = "https://www.sec.gov"


class SECClient:
    """SEC Submissions와 Company Facts API 호출을 담당하는 HTTP client다."""

    def __init__(self, user_agent, min_request_interval=0.2, timeout=30):
        """SEC 연락처 User-Agent와 요청 간격·timeout을 설정한다."""
        self.session = build_http_session(user_agent)
        self.min_request_interval = min_request_interval
        self.timeout = timeout
        self.last_request_at = 0.0

    def _get_json(self, url):
        """요청 간격을 준수해 SEC JSON을 조회하고 HTTP 오류를 호출자에게 전달한다."""
        elapsed = time.monotonic() - self.last_request_at
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)

        response = self.session.get(url, timeout=self.timeout)
        self.last_request_at = time.monotonic()
        response.raise_for_status()
        return response.json()

    def get_submissions(self, cik):
        """CIK 기준 기업정보와 최근 공시 목록을 조회한다."""
        return self._get_json(f"{SEC_DATA_URL}/submissions/CIK{cik}.json")

    def get_company_tickers(self):
        """SEC가 공개하는 상장기업 CIK·ticker·기업명 마스터를 조회한다."""
        return self._get_json(f"{SEC_WWW_URL}/files/company_tickers.json")

    def get_company_tickers_with_exchange(self):
        """SEC 상장기업 CIK·ticker·거래소 배열을 함께 제공하는 마스터를 조회한다."""
        return self._get_json(
            f"{SEC_WWW_URL}/files/company_tickers_exchange.json"
        )

    def get_historical_submission_page(self, file_name):
        """최근 1,000건 이전 공시가 담긴 SEC 과거 submissions 파일을 조회한다."""
        return self._get_json(f"{SEC_DATA_URL}/submissions/{file_name}")

    def get_company_facts(self, cik):
        """CIK 기준 표준 XBRL Company Facts 전체 이력을 조회한다."""
        return self._get_json(
            f"{SEC_DATA_URL}/api/xbrl/companyfacts/CIK{cik}.json"
        )
