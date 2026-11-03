"""Keyless fixed-endpoint transport. Retry decisions and budgets belong to the durable worker."""
import time
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter

RSS_URL = "https://news.google.com/rss/search"
GKG_ROOT = "https://data.gdeltproject.org/gdeltv2/"


class NewsFetchError(RuntimeError):
    def __init__(self, code, retryable=False, retry_after=0):
        super().__init__(code)
        self.code, self.retryable, self.retry_after = code, retryable, retry_after


class NewsHTTP:
    def __init__(self, session=None):
        self.session = session or requests.Session()
        self.session.trust_env = False
        self.session.mount("https://", HTTPAdapter(max_retries=0))
        self.session.headers.update({"User-Agent": "RevestLab-NewsCollector/1.0", "Accept-Encoding": "identity"})

    def close(self):
        self.session.close()

    def fetch(self, provider, unit, query=None):
        if provider == "RSS":
            # Strict unit/query validation is also performed before any worker DB mutation.
            from datetime import date, timedelta
            day = date.fromisoformat(unit)
            url, limit = RSS_URL, 1024**2
            params = {"q": f"{query} after:{day-timedelta(days=1)} before:{day+timedelta(days=1)}",
                      "hl": "en-US", "gl": "US", "ceid": "US:en"}
        elif provider == "GDELT":
            stamp = datetime.strptime(unit, "%Y%m%d%H%M%S")
            if stamp.minute % 15 or stamp.second or stamp.year < 2015:
                raise ValueError("INVALID_GKG_UNIT")
            url, limit, params = GKG_ROOT + unit + ".gkg.csv.zip", 32*1024**2, None
        else:
            raise ValueError("UNKNOWN_NEWS_PROVIDER")
        started = time.monotonic()
        try:
            with self.session.get(url, params=params, stream=True, timeout=(5, 60), allow_redirects=False) as response:
                if response.status_code != 200:
                    # No response bodies, headers, redirects, or private URLs in errors.
                    retry_after = response.headers.get("Retry-After", "")
                    delay = min(int(retry_after), 60) if retry_after.isdigit() else 0
                    raise NewsFetchError(f"NEWS_HTTP_{response.status_code}",
                                         response.status_code in {408, 429, 500, 502, 503, 504}, delay)
                length = response.headers.get("Content-Length", "")
                if length.isdigit() and int(length) > limit:
                    raise NewsFetchError("NEWS_RESPONSE_SIZE_LIMIT")
                raw = bytearray()
                for chunk in response.iter_content(65536):
                    raw.extend(chunk)
                    if len(raw) > limit:
                        raise NewsFetchError("NEWS_RESPONSE_SIZE_LIMIT")
                    if time.monotonic() - started > 120:
                        raise NewsFetchError("NEWS_RESPONSE_DEADLINE", True)
                return bytes(raw), {"endpoint": url, "http": 200, "bytes": len(raw),
                                    "receivedAt": datetime.now(timezone.utc).isoformat()}
        except (requests.Timeout, requests.ConnectionError):
            raise NewsFetchError("NEWS_TRANSPORT_UNAVAILABLE", True) from None
        except requests.RequestException:
            raise NewsFetchError("NEWS_TRANSPORT_INVALID") from None
