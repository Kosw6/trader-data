import os
import time
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from collectors.common import build_http_session


KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
KIS_TOKEN_PATH = "/oauth2/tokenP"
KIS_DAILY_PRICE_PATH = "/uapi/overseas-price/v1/quotations/dailyprice"
KIS_DAILY_PRICE_TR_ID = "HHDFS76240000"


class KISAPIError(RuntimeError):
    """KIS HTTP 응답 또는 업무 응답이 정상적이지 않을 때 발생한다."""


class KISClient:
    """한국투자증권 해외주식 일봉 API 호출을 담당하는 경량 클라이언트다."""

    def __init__(
        self,
        app_key,
        app_secret,
        access_token=None,
        access_token_expires_at=None,
        timeout=30,
        min_request_interval=0.12,
        max_retries=4,
    ):
        """인증 정보와 요청 간격을 설정하고 재시도 가능한 HTTP 세션을 만든다."""
        if not app_key or not app_secret:
            raise ValueError("KIS APPKEY and APPSECRET are required")
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token = access_token
        self.access_token_expires_at = parse_token_expiry(access_token_expires_at)
        self.timeout = timeout
        self.min_request_interval = min_request_interval
        self.last_request_at = 0.0
        self.session = build_http_session(
            "TraderDataBot-KR/1.0",
            total_retries=max_retries,
        )
        if max_retries == 0:
            # Return the first failed HTTP response, instead of losing its status
            # inside urllib3 MaxRetryError -> requests RetryError (even at zero retries).
            for adapter in self.session.adapters.values():
                adapter.max_retries = adapter.max_retries.new(raise_on_status=False)

    @classmethod
    def from_env(cls, **overrides):
        """현재 환경 또는 .env에 로드된 KIS 인증 값으로 클라이언트를 생성한다."""
        return cls(
            app_key=os.getenv("APPKEY"),
            app_secret=os.getenv("APPSECRET"),
            access_token=os.getenv("ACCESS_TOKEN"),
            access_token_expires_at=os.getenv("ACCESS_TOKEN_TOKEN_EXPIRED"),
            **overrides,
        )

    def _wait_for_rate_limit(self):
        """연속 호출 사이에 최소 간격을 두어 API 과호출 가능성을 낮춘다."""
        elapsed = time.monotonic() - self.last_request_at
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)

    def _request(self, method, url, **kwargs):
        """공통 timeout과 요청 간격을 적용한 뒤 HTTP 오류를 호출자에게 전달한다."""
        self._wait_for_rate_limit()
        try:
            response = self.session.request(method, url, timeout=self.timeout, **kwargs)
        finally:
            # Pace failed requests too; exceptions must not create an unpaced burst.
            self.last_request_at = time.monotonic()
        response.raise_for_status()
        return response

    def _token_is_usable(self):
        """저장된 토큰이 존재하고 만료까지 1분 이상 남았는지 확인한다."""
        if not self.access_token:
            return False
        if self.access_token_expires_at is None:
            return True
        return datetime.now() + timedelta(minutes=1) < self.access_token_expires_at

    def get_access_token(self):
        """유효한 기존 토큰을 재사용하고 필요할 때만 새 접근 토큰을 발급한다."""
        if self._token_is_usable():
            return self.access_token

        response = self._request(
            "POST",
            f"{KIS_BASE_URL}{KIS_TOKEN_PATH}",
            headers={"Content-Type": "application/json; charset=utf-8"},
            json={
                "grant_type": "client_credentials",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
            },
        )
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise KISAPIError(f"KIS token response has no access_token: {payload}")

        self.access_token = token
        self.access_token_expires_at = parse_token_expiry(
            payload.get("access_token_token_expired")
        )
        return token

    def get_daily_price_page(
        self,
        exchange_code,
        symbol,
        end_date,
        adjusted=False,
        continuation="",
    ):
        """조회 기준일 이전의 해외주식 일봉 한 페이지와 원본 응답을 반환한다."""
        if type(adjusted) is not bool:
            raise ValueError("수정주가 여부는 boolean이어야 합니다.")
        token = self.get_access_token()
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "tr_id": KIS_DAILY_PRICE_TR_ID,
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_cont": continuation,
        }
        params = {
            "AUTH": "",
            "EXCD": exchange_code.strip().upper(),
            "SYMB": symbol.strip().upper(),
            "GUBN": "0",
            "BYMD": end_date.strftime("%Y%m%d"),
            "MODP": "1" if adjusted else "0",
        }
        response = self._request(
            "GET",
            f"{KIS_BASE_URL}{KIS_DAILY_PRICE_PATH}",
            headers=headers,
            params=params,
        )
        payload = response.json()
        if str(payload.get("rt_cd", "0")) != "0":
            raise KISAPIError(
                f"KIS daily price failed: "
                f"{payload.get('msg_cd')} {payload.get('msg1')}"
            )

        bars = [
            normalize_daily_price(item)
            for item in payload.get("output2", [])
            if item.get("xymd")
        ]
        return {
            "bars": bars,
            "raw_payload": payload,
            "tr_cont": response.headers.get("tr_cont", ""),
            "requested_end_date": end_date,
        }

    def get_daily_prices(
        self,
        exchange_code,
        symbol,
        start_date,
        end_date,
        adjusted=False,
        max_pages=100,
    ):
        """일봉 페이지를 과거 방향으로 이동하며 요청 기간의 값과 원본 페이지를 모은다."""
        if end_date < start_date:
            raise ValueError("end_date must be on or after start_date")

        cursor = end_date
        bars_by_date = {}
        raw_pages = []
        seen_cursors = set()

        for _page_number in range(1, max_pages + 1):
            if cursor in seen_cursors:
                raise KISAPIError(f"KIS pagination cursor repeated: {cursor}")
            seen_cursors.add(cursor)

            page = self.get_daily_price_page(
                exchange_code=exchange_code,
                symbol=symbol,
                end_date=cursor,
                adjusted=adjusted,
            )
            raw_pages.append(
                {
                    "requested_end_date": cursor.isoformat(),
                    "tr_cont": page["tr_cont"],
                    "payload": page["raw_payload"],
                }
            )
            bars = page["bars"]
            if not bars:
                break

            for bar in bars:
                if start_date <= bar["date"] <= end_date:
                    bars_by_date[bar["date"]] = bar

            oldest_date = min(bar["date"] for bar in bars)
            if oldest_date <= start_date:
                break

            next_cursor = oldest_date - timedelta(days=1)
            if next_cursor >= cursor:
                raise KISAPIError(
                    f"KIS pagination did not move backward: {cursor} -> {next_cursor}"
                )
            cursor = next_cursor
        else:
            raise KISAPIError(
                f"KIS daily price exceeded max_pages={max_pages}: "
                f"{exchange_code}/{symbol} {start_date}..{end_date}"
            )

        return {
            "bars": [bars_by_date[key] for key in sorted(bars_by_date)],
            "raw_pages": raw_pages,
        }


def parse_token_expiry(value):
    """KIS 토큰 만료 문자열을 로컬 timezone 기준 naive datetime으로 변환한다."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(str(value), pattern)
        except ValueError:
            continue
    return None


def parse_decimal(value, field_name):
    """빈 값은 None으로 두고 숫자 문자열은 정확한 Decimal 값으로 변환한다."""
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise KISAPIError(f"invalid decimal for {field_name}: {value!r}") from error


def normalize_daily_price(item):
    """KIS output2 한 행을 서비스가 검증하기 쉬운 일봉 사전으로 정규화한다."""
    try:
        trade_date = datetime.strptime(str(item["xymd"]), "%Y%m%d").date()
    except (KeyError, ValueError) as error:
        raise KISAPIError(f"invalid KIS daily date: {item.get('xymd')!r}") from error

    return {
        "date": trade_date,
        "open": parse_decimal(item.get("open"), "open"),
        "high": parse_decimal(item.get("high"), "high"),
        "low": parse_decimal(item.get("low"), "low"),
        "close": parse_decimal(item.get("clos"), "close"),
        "volume": parse_decimal(item.get("tvol"), "volume"),
        "raw": item,
    }
