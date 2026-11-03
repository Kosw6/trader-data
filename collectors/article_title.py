"""Bounded metadata-only publisher fetches; no cookies, proxy, JavaScript or article body storage."""
import hashlib
import http.client
import ipaddress
import queue
import re
import socket
import ssl
import threading
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urlsplit, urlunsplit, urljoin, parse_qsl
from urllib.robotparser import RobotFileParser

AGENT = "RevestLab-NewsTitle/1.0"
MAX_BYTES = 512 * 1024


class TitleUnavailable(RuntimeError):
    pass


def checked_url(url):
    if not isinstance(url, str) or len(url) > 8192 or re.search(r"[\s\\\x00-\x1f\x7f]", url):
        raise TitleUnavailable("UNSAFE_URL")
    try:
        p = urlsplit(url)
        host = (p.hostname or "").encode("idna").decode("ascii").lower()
        port = p.port
    except (ValueError, UnicodeError):
        raise TitleUnavailable("UNSAFE_URL") from None
    if (p.scheme not in {"http", "https"} or p.username is not None or p.password is not None or
            port not in {None, 80 if p.scheme == "http" else 443} or
            not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", host) or "." not in host or
            host.endswith((".local", ".localhost", ".internal", ".test", ".invalid"))):
        raise TitleUnavailable("UNSAFE_URL")
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        raise TitleUnavailable("IP_LITERAL_BLOCKED")
    if any(re.search(r"token|secret|password|signature|credential|api.?key", k, re.I) for k, _ in parse_qsl(p.query)):
        raise TitleUnavailable("CREDENTIAL_URL_BLOCKED")
    return urlunsplit((p.scheme, host, p.path or "/", p.query, ""))


def public_addresses(host, timeout):
    # A daemon lookup bounds caller latency even on a stalled system resolver.
    result = queue.Queue(maxsize=1)
    def resolve():
        try:
            result.put(socket.getaddrinfo(host, None, type=socket.SOCK_STREAM))
        except OSError:
            result.put(None)
    threading.Thread(target=resolve, daemon=True).start()
    try:
        rows = result.get(timeout=max(.01, timeout))
    except queue.Empty:
        raise TitleUnavailable("DNS_TIMEOUT") from None
    if not rows:
        raise TitleUnavailable("DNS_UNAVAILABLE")
    addresses = sorted({r[4][0] for r in rows})
    if any(not ipaddress.ip_address(ip).is_global or ipaddress.ip_address(ip).is_multicast or
           ipaddress.ip_address(ip).is_reserved for ip in addresses):
        raise TitleUnavailable("NON_PUBLIC_ADDRESS_BLOCKED")
    return addresses


class _TLS(http.client.HTTPSConnection):
    def __init__(self, host, address, timeout):
        super().__init__(host, timeout=timeout, context=ssl.create_default_context())
        self.address = address

    def connect(self):
        # Connect to the already validated IP; certificate/SNI still use the original hostname.
        raw = socket.create_connection((self.address, 443), self.timeout)
        try:
            if hasattr(self, "deadline"):
                remaining = self.deadline - self.clock()
                if remaining <= 0: raise TitleUnavailable("TITLE_DEADLINE")
                raw.settimeout(min(4, remaining))
            self.sock = self._context.wrap_socket(raw, server_hostname=self.host)
        except BaseException:
            raw.close()
            raise


class _Titles(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.values = {}; self.in_title = False; self.parts = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag.lower() == "meta":
            name = (attrs.get("property") or attrs.get("name") or "").lower()
            if name in {"og:title", "twitter:title"} and attrs.get("content"):
                self.values.setdefault(name, attrs["content"])
        if tag.lower() == "title":
            self.in_title = True

    def handle_endtag(self, tag):
        if tag.lower() == "title": self.in_title = False

    def handle_data(self, data):
        if self.in_title: self.parts.append(data)


def extract_title(raw, content_type="text/html; charset=utf-8"):
    match = re.search(r"charset\s*=\s*[\"']?([\w-]+)", content_type, re.I)
    encoding = match[1] if match else "utf-8"
    try:
        text = raw.decode(encoding, errors="replace")
    except LookupError:
        text = raw.decode("utf-8", errors="replace")
    parser = _Titles(); parser.feed(text)
    parser.values["html:title"] = " ".join(parser.parts)
    for method in ("og:title", "twitter:title", "html:title"):
        value = re.sub(r"\s+", " ", parser.values.get(method, "")).strip()
        # Never save challenge/login/error pages as historical article titles.
        if (8 <= len(value) <= 1000 and "\ufffd" not in value and
                not re.search(r"access denied|just a moment|page not found|captcha|sign in|log in|404\b|robot check|verify you are human|enable javascript", value, re.I)):
            return value, method
    raise TitleUnavailable("ARTICLE_TITLE_NOT_FOUND")


class ArticleTitleClient:
    def __init__(self, clock=time.monotonic, sleep=time.sleep):
        self.clock, self.sleep = clock, sleep
        self.robots = {}; self.last_host = {}; self.requests = 0

    def _get(self, url, deadline, cap, check):
        url = checked_url(url); p = urlsplit(url)
        if self.clock() >= deadline: raise TitleUnavailable("TITLE_DEADLINE")
        if self.requests >= 40: raise TitleUnavailable("TITLE_REQUEST_BUDGET")
        check()
        delay = max(0, self.last_host.get(p.hostname, -100) + 2 - self.clock())
        if self.clock() + delay >= deadline: raise TitleUnavailable("TITLE_DEADLINE")
        if delay: self.sleep(delay)
        check()
        addresses = public_addresses(p.hostname, min(3, max(.01, deadline - self.clock())))
        timeout = min(4, max(.01, deadline - self.clock()))
        conn = (_TLS(p.hostname, addresses[0], timeout) if p.scheme == "https" else
                http.client.HTTPConnection(addresses[0], 80, timeout=timeout))
        conn.deadline, conn.clock = deadline, self.clock
        self.requests += 1; self.last_host[p.hostname] = self.clock()
        try:
            conn.request("GET", urlunsplit(("", "", p.path or "/", p.query, "")), headers={
                "Host": p.hostname, "User-Agent": AGENT, "Accept": "text/html,text/plain;q=0.5", "Accept-Encoding": "identity"})
            remaining = deadline - self.clock()
            if remaining <= 0: raise TitleUnavailable("TITLE_DEADLINE")
            if conn.sock: conn.sock.settimeout(min(4, remaining))
            response = conn.getresponse()
            headers = {k.lower(): v for k, v in response.getheaders()}
            if response.status != 200: return response.status, headers, b""
            if headers.get("content-encoding", "identity").lower() != "identity":
                raise TitleUnavailable("TITLE_ENCODING_UNSUPPORTED")
            try:
                if int(headers.get("content-length", "0")) > cap: raise TitleUnavailable("TITLE_SIZE_LIMIT")
            except ValueError:
                raise TitleUnavailable("TITLE_HEADERS_INVALID") from None
            output = bytearray()
            while True:
                check()
                remaining = deadline - self.clock()
                if remaining <= 0: raise TitleUnavailable("TITLE_DEADLINE")
                if conn.sock: conn.sock.settimeout(min(4, remaining))
                chunk = response.read1(min(8192, cap + 1 - len(output)))
                if not chunk: break
                output.extend(chunk)
                if len(output) > cap: raise TitleUnavailable("TITLE_SIZE_LIMIT")
            return response.status, headers, bytes(output)
        except (OSError, http.client.HTTPException):
            raise TitleUnavailable("TITLE_TRANSPORT_UNAVAILABLE") from None
        finally:
            conn.close()

    def _allowed(self, url, deadline, check):
        p = urlsplit(url); origin = f"{p.scheme}://{p.netloc}"
        if origin not in self.robots:
            robots_url = origin + "/robots.txt"
            for _ in range(3):
                status, headers, raw = self._get(robots_url, deadline, 128 * 1024, check)
                if status not in {301, 302, 303, 307, 308}: break
                target = checked_url(urljoin(robots_url, headers.get("location", "")))
                if urlsplit(robots_url).scheme == "https" and urlsplit(target).scheme != "https":
                    raise TitleUnavailable("TLS_DOWNGRADE_BLOCKED")
                robots_url = target
            else: raise TitleUnavailable("ROBOTS_REDIRECT_LIMIT")
            if status in {404, 410}: self.robots[origin] = True
            elif status == 200:
                robot = RobotFileParser(); robot.parse(raw.decode("utf-8", errors="replace").splitlines())
                self.robots[origin] = robot
            else: raise TitleUnavailable("ROBOTS_UNAVAILABLE")
        rule = self.robots[origin]
        if rule is not True:
            if not rule.can_fetch(AGENT, url): raise TitleUnavailable("ROBOTS_DISALLOWED")
            # Respect larger publisher pacing requests; do not hold a worker for a long crawl delay.
            delay = rule.crawl_delay(AGENT) or 0
            rate = rule.request_rate(AGENT)
            if rate and rate.requests <= 0: raise TitleUnavailable("ROBOTS_INVALID_RULE")
            if delay > 2 or (rate and rate.seconds / rate.requests > 2):
                raise TitleUnavailable("ROBOTS_SLOW_CRAWL_REVIEW")

    def fetch(self, url, check=lambda: None, deadline=None):
        deadline = min(deadline or self.clock() + 12, self.clock() + 12)
        try:
            current = checked_url(url)
            upgraded = urlsplit(current).scheme == "http"
            if upgraded:
                p = urlsplit(current)
                current = urlunsplit(("https", p.netloc, p.path, p.query, ""))
            for hop in range(3):
                self._allowed(current, deadline, check)
                status, headers, raw = self._get(current, deadline, MAX_BYTES, check)
                if status in {301, 302, 303, 307, 308}:
                    target = checked_url(urljoin(current, headers.get("location", "")))
                    if urlsplit(current).scheme == "https" and urlsplit(target).scheme != "https":
                        raise TitleUnavailable("TLS_DOWNGRADE_BLOCKED")
                    current = target
                    continue
                if status != 200: raise TitleUnavailable(f"TITLE_HTTP_{status}")
                if "text/html" not in headers.get("content-type", "").lower():
                    raise TitleUnavailable("TITLE_NOT_HTML")
                if hop and urlsplit(current).path == "/" and urlsplit(url).path not in {"", "/"}:
                    raise TitleUnavailable("ARTICLE_REDIRECTED_TO_HOME")
                title, method = extract_title(raw, headers["content-type"])
                return dict(status="RECOVERED", title=title, method=method, finalUrl=current,
                            receivedAt=datetime.now(timezone.utc).isoformat(), responseSha256=hashlib.sha256(raw).hexdigest(),
                            responseBytes=len(raw), robots="ALLOWED", redirects=hop,
                            httpsUpgrade=upgraded,
                            scope="CURRENT_PAGE_METADATA_NOT_HISTORICAL_BODY_VERIFICATION")
            raise TitleUnavailable("TITLE_REDIRECT_LIMIT")
        except TitleUnavailable as error:
            return {"status": "REVIEW_REQUIRED", "reason": str(error)}
