from collectors.common import build_http_session


BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_CALENDAR_URL = "https://www.bls.gov/schedule/news_release/bls.ics"


class BLSClient:
    """BLS 발표 캘린더와 Timeseries API 호출을 담당하는 HTTP client다."""

    def __init__(self, user_agent="TraderDataBot-KR/1.0", timeout=30):
        """BLS 요청에 사용할 User-Agent와 timeout을 설정한다."""
        self.session = build_http_session(user_agent)
        self.timeout = timeout

    def get_calendar(self):
        """BLS 공식 발표 일정을 ICS 원본 바이트로 조회한다."""
        response = self.session.get(BLS_CALENDAR_URL, timeout=self.timeout)
        response.raise_for_status()
        return response.content

    def get_timeseries(self, series_ids, start_year, end_year, api_key=None):
        """여러 Series ID의 지정 연도 범위를 조회하고 BLS 응답 상태를 검증한다."""
        payload = {
            "seriesid": list(series_ids),
            "startyear": str(start_year),
            "endyear": str(end_year),
        }
        if api_key:
            payload["registrationkey"] = api_key

        response = self.session.post(
            BLS_API_URL,
            json=payload,
            timeout=self.timeout,
        )
        response.raise_for_status()
        result = response.json()
        if result.get("status") != "REQUEST_SUCCEEDED":
            raise RuntimeError(
                f"BLS API request failed: status={result.get('status')} "
                f"messages={result.get('message')}"
            )
        return result
