"""Stage-one, process-local HTTP policy. No DB writes or job restart authority.

One budget is shared by every page and token request of a collector item. A
controller-owned persistent budget and distributed quota gate are stage two.
"""

import math
import random
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from enum import Enum
from types import SimpleNamespace

import requests


class Category(str, Enum):
    TRANSIENT = "TRANSIENT_TRANSPORT"
    RATE_LIMIT = "RATE_LIMIT"
    AUTH = "AUTH_BLOCKED"
    EMPTY = "EMPTY_UNRESOLVED"
    NO_CHANGE = "NO_CHANGE_VERIFIED"
    NOT_FOUND = "SOURCE_NOT_FOUND"
    QUALITY = "QUALITY_OR_SEMANTIC_CONFLICT"
    COMMIT = "COMMIT_UNCERTAIN"
    IMPLEMENTATION = "IMPLEMENTATION_ERROR"
    RESOURCE = "RESOURCE_BLOCKED"
    PAUSED = "USER_PAUSED"
    UNKNOWN = "UNCLASSIFIED"


@dataclass(frozen=True)
class Failure:
    category: Category
    http_status: int | None = None
    provider_code: str | None = None
    retry_after: float | None = None
    invalid_retry_after: bool = False

    def metadata(self):
        # Never include exception text, URL, payload, request or headers.
        return {
            "policyVersion": "bounded-v1",
            "failureCategory": self.category.value,
            "httpStatus": self.http_status,
            "providerErrorCode": self.provider_code,
            "retryAfterSeconds": self.retry_after,
            "invalidRetryAfter": self.invalid_retry_after,
        }


class RequestPolicyError(RuntimeError):
    def __init__(self, failure, reason="non_retryable", *, attempts=0, retries=0):
        self.failure = failure
        self.reason = reason
        self.attempts = attempts
        self.retries = retries
        # Preserve the SEC collector's status-only 404 compatibility, without
        # retaining a Response containing credentials or a sensitive request.
        self.response = SimpleNamespace(status_code=failure.http_status)
        super().__init__(f"{failure.category.value}: {reason}")

    def metadata(self):
        return {
            **self.failure.metadata(),
            "stopReason": self.reason,
            "requestAttempts": self.attempts,
            "retriesUsed": self.retries,
        }


def parse_retry_after(value, now=None):
    """Return seconds and validity; never truncate a long provider wait."""
    if value is None:
        return None, False
    if not isinstance(value, str) or len(value) > 128 or not value.strip():
        return None, True
    value = value.strip()
    try:
        if re.fullmatch(r"[0-9]+", value):
            seconds = float(value)
        else:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                return None, True
            seconds = max(
                0.0, (date - (now or datetime.now(timezone.utc))).total_seconds()
            )
        if not math.isfinite(seconds):
            return None, True
        return seconds, False
    except (ValueError, OverflowError, TypeError):
        return None, True


def classify(*, error=None, response=None, provider=None, outcome=None, now=None):
    """Classify facts, not exception prose. Explicit semantic outcomes win."""
    if outcome is not None:
        return Failure(Category(outcome))
    if isinstance(error, RequestPolicyError):
        return error.failure
    if response is None and error is not None:
        response = getattr(error, "response", None)
    status = getattr(response, "status_code", None)
    status = status if type(status) is int and 100 <= status <= 599 else None
    headers = getattr(response, "headers", {})
    retry_after, invalid = parse_retry_after(headers.get("Retry-After"), now)
    code = None
    business_failure = False
    if provider == "KIS" and response is not None:
        try:
            body = response.json()
            if isinstance(body, dict):
                candidate = body.get("msg_cd")
                if isinstance(candidate, str) and re.fullmatch(
                    r"[A-Z0-9_]{1,32}", candidate
                ):
                    code = candidate
                business_failure = str(body.get("rt_cd", "0")) != "0"
        except (ValueError, TypeError):
            pass
    if status == 429 or (provider == "KIS" and code == "EGW00201"):
        category = Category.RATE_LIMIT
    elif status in (401, 403):
        category = Category.AUTH
    elif status == 404:
        category = Category.NOT_FOUND
    elif status in (408, 500, 502, 503, 504):
        category = Category.TRANSIENT
    elif business_failure or (status is not None and status >= 400):
        category = Category.UNKNOWN
    elif isinstance(
        error, (requests.exceptions.SSLError, requests.exceptions.InvalidURL)
    ):
        category = Category.IMPLEMENTATION
    elif isinstance(
        error,
        (
            requests.ConnectionError,
            requests.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ),
    ):
        category = Category.TRANSIENT
    elif isinstance(error, requests.RequestException):
        category = Category.UNKNOWN
    elif error is not None:
        category = (
            Category.IMPLEMENTATION
            if isinstance(error, (ValueError, OSError))
            else Category.UNKNOWN
        )
    else:
        return None
    return Failure(category, status, code, retry_after, invalid)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3  # per HTTP operation, including first attempt
    max_retries: int = 2  # across all operations/pages/token requests in this item
    max_requests: int = 256
    max_elapsed: float = 600.0
    base_delay: float = 1.0
    max_delay: float = 30.0
    rate_limit_delay: float = 60.0

    def __post_init__(self):
        for name in ("max_attempts", "max_retries", "max_requests"):
            value = getattr(self, name)
            if type(value) is not int or value < (0 if name == "max_retries" else 1):
                raise ValueError(f"Invalid {name}")
        for name in ("max_elapsed", "base_delay", "max_delay", "rate_limit_delay"):
            value = getattr(self, name)
            if (
                not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"Invalid {name}")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must cover base_delay")

    def delay(self, failure, retry_index, jitter):
        delay = min(self.max_delay, self.base_delay * 2 ** min(retry_index, 30))
        delay *= 0.5 + 0.5 * jitter
        if failure.category == Category.RATE_LIMIT:
            delay = max(delay, self.rate_limit_delay)
        return max(delay, failure.retry_after or 0.0)


class RequestGate:
    """Serialize one process's provider calls, pace failures and share cooldown.

    This deliberately makes no multi-process or distributed quota guarantee.
    """

    def __init__(self, interval=1.0, *, clock=time.monotonic):
        if not math.isfinite(interval) or interval < 0:
            raise ValueError("Invalid request interval")
        self.interval = interval
        self.clock = clock
        self.not_before = 0.0
        self.blocked = False
        self.lock = threading.Lock()


class RequestExecutor:
    """Accept only a single HTTP send callback; never wrap ETL or DB writes."""

    def __init__(
        self,
        policy=None,
        gate=None,
        *,
        clock=time.monotonic,
        sleep=time.sleep,
        jitter=random.random,
        stopped=lambda: False,
    ):
        self.policy = policy or RetryPolicy()
        self.gate = gate or RequestGate(clock=clock)
        self.clock, self.sleep, self.jitter, self.stopped = (
            clock,
            sleep,
            jitter,
            stopped,
        )
        self.started = clock()
        self.requests = 0
        self.retries = 0
        self.history = []  # allowlisted attempt receipts, bounded by max_requests

    def abort(self, failure, reason):
        raise RequestPolicyError(
            failure, reason, attempts=self.requests, retries=self.retries
        ) from None

    def check(self):
        if self.stopped():
            self.abort(Failure(Category.PAUSED), "user_paused")
        if self.clock() - self.started >= self.policy.max_elapsed:
            self.abort(Failure(Category.RESOURCE), "elapsed_budget")

    def wait_until(self, due, failure):
        if due >= self.started + self.policy.max_elapsed:
            self.abort(failure, "wait_exceeds_budget")
        while self.clock() < due:
            self.check()
            self.sleep(min(0.25, due - self.clock()))
        self.check()

    def request(self, send, *, method="GET", provider=None):
        method = method.upper()
        local_attempt = 0
        retry_due = 0.0
        last_failure = Failure(Category.RESOURCE)
        while True:
            self.check()
            with self.gate.lock:
                if self.gate.blocked:
                    self.abort(Failure(Category.RATE_LIMIT), "quota_review_required")
                self.wait_until(max(retry_due, self.gate.not_before), last_failure)
                if self.requests >= self.policy.max_requests:
                    self.abort(last_failure, "request_budget")
                if local_attempt:
                    if self.retries >= self.policy.max_retries:
                        self.abort(last_failure, "retry_budget")
                    self.retries += 1
                self.requests += 1
                local_attempt += 1
                response = None
                failure = None
                try:
                    remaining = self.policy.max_elapsed - (self.clock() - self.started)
                    response = send(remaining)
                    failure = classify(response=response, provider=provider)
                except requests.RequestException as error:
                    failure = classify(error=error, provider=provider)
                    response = getattr(error, "response", None)
                finally:
                    self.gate.not_before = max(
                        self.gate.not_before, self.clock() + self.gate.interval
                    )
                self.history.append(
                    {
                        "attempt": self.requests,
                        "retry": local_attempt > 1,
                        "outcome": failure.metadata() if failure else "HTTP_OK",
                    }
                )
                if failure is None:
                    return response
                if response is not None:
                    response.close()
                # Honor server waits even when the local retry budget is exhausted.
                if failure.invalid_retry_after:
                    self.gate.blocked = True
                delay = self.policy.delay(failure, self.retries, self.jitter())
                if (
                    failure.category == Category.RATE_LIMIT
                    or failure.retry_after is not None
                ):
                    self.gate.not_before = max(
                        self.gate.not_before, self.clock() + delay
                    )
                last_failure = failure
            if failure.invalid_retry_after:
                self.abort(failure, "invalid_retry_after")
            if method not in ("GET", "HEAD"):
                self.abort(failure, "non_idempotent_method")
            if failure.category not in (Category.TRANSIENT, Category.RATE_LIMIT):
                self.abort(failure, "non_retryable")
            if (
                local_attempt >= self.policy.max_attempts
                or self.retries >= self.policy.max_retries
            ):
                self.abort(failure, "retry_budget")
            retry_due = self.clock() + delay
