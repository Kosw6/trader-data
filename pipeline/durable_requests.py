"""Opt-in Controller-backed request permissions. No control-plane retries.

Every provider send needs a committed reservation. Lost responses and lost leases
stop the caller; they are never interpreted as permission to send again.
"""

import hashlib
import json
import math
import threading
import time
import uuid
from datetime import datetime
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter

from pipeline.request_policy import Category, Failure, RequestPolicyError, classify


class CoordinationError(RuntimeError):
    def __init__(self, code="coordination_unavailable"):
        self.code = (
            code
            if code
            in {
                "quota_wait",
                "lease_lost",
                "budget_exhausted",
                "review_required",
                "already_reserved",
                "already_committed",
                "commit_uncertain",
                "contract_conflict",
                "invalid_contract",
                "coordination_unavailable",
            }
            else "coordination_unavailable"
        )
        super().__init__(self.code)  # Never reflect a URL, header or remote prose.


def canonical_hash(value):
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode()
    ).hexdigest()


class Controller:
    def __init__(self, url, key, *, proxy_prefix=""):
        parts = urlsplit(url)
        if (
            not key
            or proxy_prefix not in ("", "/api/controller")
            or parts.username
            or parts.password
            or parts.query
            or parts.fragment
            or parts.path not in ("", "/")
            or not parts.hostname
            or (
                parts.scheme != "https"
                and not (
                    parts.scheme == "http"
                    and parts.hostname in ("127.0.0.1", "localhost", "::1")
                )
            )
        ):
            raise CoordinationError("invalid_contract")
        self.url = url.rstrip("/") + proxy_prefix + "/api/v1/admin/pipeline/recovery/work"
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers["X-Admin-API-Key"] = key
        for scheme in ("http://", "https://"):
            self.session.mount(scheme, HTTPAdapter(max_retries=0))
        self.lock = threading.Lock()

    def call(self, method, path="", body=None):
        with self.lock:
            try:
                with self.session.request(
                    method, self.url + path, json=body, timeout=5, allow_redirects=False
                ) as response:
                    payload = response.json()
                    if not isinstance(payload, dict):
                        raise TypeError()
                    if response.status_code != 200:
                        code = payload.get("error", {}).get("code")
                        if response.status_code != 409:
                            code = "coordination_unavailable"
                        raise CoordinationError(code)
                    return payload
            except CoordinationError:
                raise
            except (requests.RequestException, ValueError, TypeError, AttributeError):
                raise CoordinationError() from None

    def close(self):
        self.session.close()


class DurableLease:
    def __init__(self, api, identity, policy, *, stopped=lambda: False):
        self.api, self.identity, self.policy = api, identity, policy
        self.stopped = stopped
        self.done = threading.Event()
        self.mutex = threading.RLock()
        self.failure = None
        self.valid_until = 0
        self.thread = None
        self.owner = uuid.uuid4().hex
        work = api.call("POST", body={"identity": identity, "policy": policy})
        if work.get("identity") != identity or work.get("policy") != policy:
            raise CoordinationError("contract_conflict")
        if work.get("phase") != "FETCH_RAW":
            raise CoordinationError("already_committed")
        self.key = work["key"]
        if len(self.key) != 64 or any(c not in "0123456789abcdef" for c in self.key):
            raise CoordinationError("invalid_contract")
        started = time.monotonic()
        self._accept(
            api.call("POST", f"/{self.key}/claim", {"owner": self.owner}), started
        )
        self.thread = threading.Thread(target=self._heartbeat, daemon=True)
        self.thread.start()

    @property
    def token(self):
        return {"workKey": self.key, "owner": self.owner, "fence": self.fence}

    def _accept(self, work, started):
        try:
            fence = work["fence"]
            if (
                work["key"] != self.key
                or work["state"] != "LEASED"
                or work["owner"] != self.owner
                or type(fence) is not int
                or fence <= 0
                or (hasattr(self, "fence") and fence != self.fence)
            ):
                raise ValueError()
            parse = lambda x: datetime.fromisoformat(x.replace("Z", "+00:00"))
            remaining = (
                min(parse(work["leaseUntil"]), parse(work["deadline"]))
                - parse(work["serverNow"])
            ).total_seconds()
            # Subtract the ENTIRE request round trip, independent of local wall clock.
            due = started + remaining - 0.1
            if due <= time.monotonic():
                raise ValueError()
        except (KeyError, ValueError, TypeError, AttributeError):
            raise CoordinationError("lease_lost") from None
        self.fence = fence
        self.valid_until = due

    def check(self):
        if self.failure or self.done.is_set() or time.monotonic() >= self.valid_until:
            self.failure = self.failure or "lease_lost"
            raise CoordinationError(self.failure)
        if self.stopped():
            self.failure = "lease_lost"
            try:
                self.api.call("POST", f"/{self.key}/pause-owned", self.token)
            except CoordinationError:
                pass  # Never retry uncertain control writes or revive the local lease.
            raise CoordinationError("lease_lost")

    def renew(self):
        with self.mutex:
            self.check()
            started = time.monotonic()
            try:
                self._accept(
                    self.api.call("POST", f"/{self.key}/renew", self.token), started
                )
            except CoordinationError as error:
                self.failure = error.code
                raise

    def _heartbeat(self):
        interval = min(5, self.policy["leaseMs"] / 3000)
        while not self.done.wait(interval):
            try:
                self.renew()
            except CoordinationError:
                return

    def reserve(self, operation):
        request_id = uuid.uuid4().hex
        while True:
            self.renew()
            try:
                reply = self.api.call(
                    "POST",
                    f"/{self.key}/requests",
                    {
                        "lease": self.token,
                        "requestId": request_id,
                        "operation": operation,
                    },
                )
                if reply.get("sendOnce") is not True:
                    raise CoordinationError()
                self.check()
                return request_id
            except CoordinationError as error:
                if error.code != "quota_wait":
                    self.failure = error.code
                    raise
                # Only a definitive quota rejection permits polling; no slot existed.
                if self.done.wait(1):
                    raise CoordinationError("lease_lost")

    def report(self, request_id, outcome, retry_after=0):
        self.check()
        try:
            reply = self.api.call(
                "POST",
                f"/{self.key}/requests/{request_id}/report",
                {"lease": self.token, "outcome": outcome, "retryAfterMs": retry_after},
            )
            if reply.get("recorded") is not True:
                raise CoordinationError()
        except CoordinationError as error:
            self.failure = error.code
            raise

    def close(self):
        self.done.set()
        if self.thread:
            self.thread.join(timeout=6)
        # No release/refund: a pending request stays pending after process loss.
        self.api.close()


class DurableSession(requests.Session):
    def __init__(self, provider, lease, user_agent, *, allowed_origins=None):
        super().__init__()
        self.provider, self.lease = provider, lease
        self.trust_env = False
        self.headers.update(
            {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}
        )
        self.allowed_origins = allowed_origins or (
            {"https://openapi.koreainvestment.com:9443"}
            if provider == "KIS"
            else {"https://data.sec.gov", "https://www.sec.gov"}
        )
        for scheme in ("http://", "https://"):
            self.mount(scheme, HTTPAdapter(max_retries=0))

    def request(self, method, url, **kwargs):
        method = method.upper()
        parts = urlsplit(url)
        if (
            f"{parts.scheme}://{parts.netloc}" not in self.allowed_origins
            or parts.username
            or parts.password
            or parts.fragment
            or parts.query
            or method not in ("GET", "POST")
            or kwargs.get("stream")
            or kwargs.get("auth")
            or kwargs.get("hooks")
        ):
            raise CoordinationError("invalid_contract")
        if method == "POST" and not (
            self.provider == "KIS" and parts.path == "/oauth2/tokenP"
        ):
            raise CoordinationError("invalid_contract")
        timeout = kwargs.pop("timeout", 30)
        if (
            type(timeout) not in (float, int)
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise CoordinationError("invalid_contract")
        kwargs["allow_redirects"] = False
        # Token bodies/authorization are NEVER persisted, even as operation hashes.
        operation = canonical_hash(
            {
                "method": method,
                "url": url,
                "params": kwargs.get("params"),
                "continuation": kwargs.get("headers", {}).get("tr_cont", ""),
            }
        )
        while True:
            request_id = self.lease.reserve(operation)
            response, failure = None, None
            try:
                self.lease.check()
                response = super().request(
                    method,
                    url,
                    timeout=min(
                        timeout,
                        max(0.01, (self.lease.valid_until - time.monotonic()) / 2),
                    ),
                    **kwargs,
                )
                failure = classify(response=response, provider=self.provider)
                if failure is None:
                    try:
                        payload = response.json()
                        if (
                            not isinstance(payload, dict)
                            or not 200 <= response.status_code < 300
                        ):
                            raise ValueError()
                        if method == "POST" and not payload.get("access_token"):
                            failure = Failure(Category.AUTH)
                    except (ValueError, TypeError):
                        failure = Failure(Category.IMPLEMENTATION)
            except requests.RequestException as error:
                failure = classify(error=error, provider=self.provider)
            except BaseException:
                if response is not None:
                    response.close()
                raise  # Leave the committed reservation pending.
            wait = math.ceil((failure.retry_after or 0) * 1000) if failure else 0
            invalid_wait = failure and (failure.invalid_retry_after or wait > 604800000)
            outcome = failure.category.value if failure else "HTTP_OK"
            if invalid_wait or (method == "POST" and failure):
                # Non-idempotent token POST failures may not be retried after restart.
                outcome = "UNCLASSIFIED"
            try:
                if invalid_wait:
                    # Unknown provider wait must block the shared quota, not release it.
                    raise CoordinationError("review_required")
                self.lease.report(request_id, outcome, wait)
                if failure is None:
                    self.lease.check()
                    return response
            except BaseException:
                if response is not None:
                    response.close()
                raise
            if response is not None:
                response.close()
            if method != "GET" or failure.category not in (
                Category.TRANSIENT,
                Category.RATE_LIMIT,
            ):
                raise RequestPolicyError(failure, "durable_review") from None
            # No local retry budget or retry sleep: next Reserve enforces DB budgets/quota.
