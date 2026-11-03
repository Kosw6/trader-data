"""Opt-in clients; legacy modules and live backfill imports remain unchanged."""

import math
import threading

import requests
from pipeline.request_policy import (
    Category,
    Failure,
    RequestExecutor,
    RequestGate,
    RequestPolicyError,
)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from collectors.kis_client import KISAPIError, KISClient
from collectors.sec_client import SECClient

_GATES = {}
_GATES_LOCK = threading.Lock()


def provider_gate(provider, interval):
    # Conservative provider-wide scope: no credentials are used as a key. This
    # shares waits across sequential items and clients within ONE process only.
    if not math.isfinite(interval) or interval < 0:
        raise ValueError("Invalid request interval")
    with _GATES_LOCK:
        gate = _GATES.setdefault(provider, RequestGate(interval=max(1.0, interval)))
        with gate.lock:
            gate.interval = max(gate.interval, interval, 1.0)
        return gate


class PolicySession(requests.Session):
    def __init__(self, provider, executor, user_agent):
        super().__init__()
        self.provider = provider
        self.executor = executor
        retry = Retry(
            total=0,
            connect=0,
            read=0,
            status=0,
            redirect=0,
            other=0,
            raise_on_status=False,
            respect_retry_after_header=False,
        )
        for scheme in ("http://", "https://"):
            self.mount(scheme, HTTPAdapter(max_retries=retry))
        self.headers.update(
            {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}
        )

    def request(self, method, url, **kwargs):
        timeout = kwargs.pop("timeout", 30.0)
        if (
            not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("A finite positive scalar timeout is required")
        kwargs["allow_redirects"] = (
            False  # redirects are extra requests and can leak auth
        )
        if kwargs.get("stream"):
            raise ValueError("Streaming is outside this JSON collector contract")

        def send(remaining):
            return super(PolicySession, self).request(
                method, url, timeout=min(timeout, remaining), **kwargs
            )

        response = self.executor.request(send, method=method, provider=self.provider)
        if 300 <= response.status_code < 400:
            response.close()
            self.executor.abort(
                Failure(Category.UNKNOWN, response.status_code), "redirect_rejected"
            )
        try:
            payload = response.json()
            if not isinstance(payload, dict):
                raise TypeError("Object payload required")
            if (
                self.provider == "KIS"
                and method.upper() == "POST"
                and not payload.get("access_token")
            ):
                response.close()
                self.executor.abort(Failure(Category.AUTH), "token_missing")
        except (ValueError, TypeError):
            response.close()
            self.executor.abort(
                Failure(Category.IMPLEMENTATION), "invalid_json_contract"
            )
        return response


class PolicyKISClient(KISClient):
    def __init__(self, *args, executor=None, **kwargs):
        kwargs["max_retries"] = 0
        super().__init__(*args, **kwargs)
        self.session.close()
        executor = executor or RequestExecutor(
            gate=provider_gate("KIS", self.min_request_interval)
        )
        self.session = PolicySession("KIS", executor, "TraderDataBot-KR/1.0")
        self.last_outcome = None

    def _request(self, method, url, **kwargs):
        # Pacing, including errors and token POST, is exclusively executor-owned.
        return self.session.request(method, url, timeout=self.timeout, **kwargs)

    def get_daily_price_page(self, *args, **kwargs):
        try:
            result = super().get_daily_price_page(*args, **kwargs)
        except KISAPIError:
            raise RequestPolicyError(
                Failure(Category.QUALITY), "invalid_price_payload"
            ) from None
        # Preserve empty raw data for the existing collection/validation path;
        # never refetch it in a tight HTTP loop or label it verified no-change.
        self.last_outcome = Failure(Category.EMPTY) if not result["bars"] else None
        return result

    def get_daily_prices(self, *args, **kwargs):
        try:
            return super().get_daily_prices(*args, **kwargs)
        except KISAPIError:
            raise RequestPolicyError(
                Failure(Category.QUALITY), "invalid_price_series"
            ) from None


class PolicySECClient(SECClient):
    def __init__(
        self, user_agent, min_request_interval=1.0, timeout=30, *, executor=None
    ):
        # Do not construct the legacy session with its hidden HTTP retries.
        self.timeout = timeout
        self.min_request_interval = min_request_interval
        executor = executor or RequestExecutor(
            gate=provider_gate("SEC", min_request_interval)
        )
        self.session = PolicySession("SEC", executor, user_agent)

    def _get_json(self, url):
        return self.session.get(url, timeout=self.timeout).json()


def kis_client(policy="legacy", **kwargs):
    if policy not in ("legacy", "bounded-v1"):
        raise ValueError("Unknown request policy")
    return (PolicyKISClient if policy == "bounded-v1" else KISClient).from_env(**kwargs)


def sec_client(policy="legacy", **kwargs):
    if policy not in ("legacy", "bounded-v1"):
        raise ValueError("Unknown request policy")
    return (PolicySECClient if policy == "bounded-v1" else SECClient)(**kwargs)


def policy_checkpoint(client):
    executor = getattr(client.session, "executor", None)
    if executor is None:
        return {}
    outcome = getattr(client, "last_outcome", None)
    return {
        "requestPolicy": {
            "version": "bounded-v1",
            "scope": "single_item_single_process",
            "requests": executor.requests,
            "retries": executor.retries,
            "attempts": list(executor.history),
            "result": outcome.metadata() if outcome else None,
        }
    }
