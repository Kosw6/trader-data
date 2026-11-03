import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
import requests

from pipeline.durable_requests import (
    Controller,
    CoordinationError,
    DurableLease,
    DurableSession,
    canonical_hash,
)
from pipeline.request_policy import RequestPolicyError
from tests.test_request_policy import response
from workers.coordinated_collector import load_manifest


class Lease:
    def __init__(self):
        self.valid_until = time.monotonic() + 60
        self.reserve = Mock(side_effect=["one", "two", "three"])
        self.report = Mock()
        self.check = Mock()


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com",
        "https://user:secret@example.com",
        "https://x/a",
        "https://x?key=secret",
    ],
)
def test_control_rejects_unsafe_url(url):
    with pytest.raises(CoordinationError):
        Controller(url, "dummy")


def test_control_failure_sanitized_no_retry():
    api = Controller("http://127.0.0.1:8090", "dummy")
    with patch.object(
        api.session, "request", side_effect=requests.ConnectionError("SECRET")
    ) as send:
        with pytest.raises(CoordinationError) as error:
            api.call("POST", body={})
        assert "SECRET" not in str(error.value)
        assert send.call_count == 1
    api.close()


def test_only_definitive_409_can_grant_quota_polling():
    api = Controller("http://127.0.0.1:8090", "dummy")
    try:
        with (
            patch.object(
                api.session,
                "request",
                return_value=response(503, {"error": {"code": "quota_wait"}}),
            ),
            pytest.raises(CoordinationError, match="coordination_unavailable"),
        ):
            api.call("POST", "/requests")
    finally:
        api.close()


@pytest.mark.parametrize(
    "status,body",
    [
        (403, {}),
        (404, {}),
        (302, {}),
        (200, []),
        (200, {"rt_cd": "1", "msg_cd": "OTHER"}),
    ],
)
def test_terminal_response_sent_once(status, body):
    lease = Lease()
    with (
        DurableSession("KIS", lease, "test") as client,
        patch.object(
            requests.Session, "request", return_value=response(status, body)
        ) as send,
    ):
        with pytest.raises(RequestPolicyError):
            client.get("https://openapi.koreainvestment.com:9443/page")
        assert send.call_count == 1
        assert lease.report.call_count == 1
        assert send.call_args.kwargs["allow_redirects"] is False


def test_retry_uses_same_operation_and_two_db_reservations():
    lease = Lease()
    with (
        DurableSession("SEC", lease, "test") as client,
        patch.object(
            requests.Session,
            "request",
            side_effect=[response(503, {}), response(200, {})],
        ) as send,
    ):
        client.get("https://data.sec.gov/submissions/CIK1.json").close()
        assert send.call_count == 2
        assert lease.reserve.call_args_list[0] == lease.reserve.call_args_list[1]
        assert [x.args[1] for x in lease.report.call_args_list] == [
            "TRANSIENT_TRANSPORT",
            "HTTP_OK",
        ]


@pytest.mark.parametrize(
    "code",
    [
        "lease_lost",
        "budget_exhausted",
        "already_reserved",
        "commit_uncertain",
        "coordination_unavailable",
    ],
)
def test_no_send_without_confirmed_permission(code):
    lease = Lease()
    lease.reserve.side_effect = CoordinationError(code)
    with (
        DurableSession("SEC", lease, "test") as client,
        patch.object(requests.Session, "request") as send,
    ):
        with pytest.raises(CoordinationError):
            client.get("https://data.sec.gov/test")
        send.assert_not_called()


def test_lost_report_does_not_replay_provider():
    lease = Lease()
    lease.report.side_effect = CoordinationError()
    with (
        DurableSession("SEC", lease, "test") as client,
        patch.object(
            requests.Session, "request", return_value=response(503, {})
        ) as send,
    ):
        with pytest.raises(CoordinationError):
            client.get("https://data.sec.gov/test")
        assert send.call_count == 1


def test_token_post_failure_requires_review_and_never_retries():
    lease = Lease()
    with (
        DurableSession("KIS", lease, "test") as client,
        patch.object(
            requests.Session, "request", return_value=response(503, {})
        ) as send,
    ):
        with pytest.raises(RequestPolicyError):
            client.post(
                "https://openapi.koreainvestment.com:9443/oauth2/tokenP",
                json={"appsecret": "SECRET"},
            )
        assert send.call_count == 1
        assert lease.report.call_args.args[1] == "UNCLASSIFIED"


@pytest.mark.parametrize("header", ["invalid", "99999999999999999999999999999999"])
def test_bad_wait_retains_pending(header):
    lease = Lease()
    r = response(429, {})
    r.headers["Retry-After"] = header
    with (
        DurableSession("SEC", lease, "test") as client,
        patch.object(requests.Session, "request", return_value=r),
    ):
        with pytest.raises(CoordinationError):
            client.get("https://data.sec.gov/test")
        lease.report.assert_not_called()


class API:
    def __init__(self):
        self.identity = {"provider": "SEC"}
        self.policy = {"leaseMs": 30000}
        self.key = "a" * 64
        self.work = {
            "key": self.key,
            "identity": self.identity,
            "policy": self.policy,
            "phase": "FETCH_RAW",
        }
        self.renew_error = False

    def call(self, method, path="", body=None):
        if not path:
            return self.work.copy()
        if path.endswith("/renew") and self.renew_error:
            raise CoordinationError()
        now = datetime.now(timezone.utc)
        return {
            **self.work,
            "owner": body["owner"],
            "fence": 1,
            "state": "LEASED",
            "leaseUntil": (now + timedelta(seconds=30)).isoformat(),
            "deadline": (now + timedelta(seconds=60)).isoformat(),
            "serverNow": now.isoformat(),
        }

    def close(self):
        pass


def test_renewal_response_loss_latches_failure():
    api = API()
    lease = DurableLease(api, api.identity, api.policy)
    try:
        api.renew_error = True
        with pytest.raises(CoordinationError):
            lease.renew()
        api.renew_error = False
        with pytest.raises(CoordinationError):
            lease.check()
    finally:
        lease.close()


def test_cancellation_stops_heartbeat_and_next_request():
    api = API()
    stopped = [False]
    lease = DurableLease(api, api.identity, api.policy, stopped=lambda: stopped[0])
    try:
        stopped[0] = True
        with pytest.raises(CoordinationError):
            lease.reserve("page")
        stopped[0] = False
        with pytest.raises(CoordinationError):
            lease.check()  # Observed cancellation cannot silently resume.
    finally:
        lease.close()


def test_manifest_pins_target_revision_quota_policy(tmp_path, monkeypatch):
    contract = {
        "target": "asset:1",
        "start": "2026-01-01",
        "end": "2026-01-02",
        "requestIntervalMs": 1000,
    }
    doc = {
        "version": "coordinated-v1",
        "generation": "approved-generation",
        "contract": contract,
        "identity": {
            "provider": "KIS",
            "target": "asset:1",
            "quotaScope": "test",
            "dataset": "price-1D",
            "start": contract["start"],
            "end": contract["end"],
            "revision": canonical_hash(
                {"generation": "approved-generation", "contract": contract}
            ),
        },
        "policy": {"leaseMs": 30000, "intervalMs": 1000},
    }
    path = tmp_path / "manifest.json"
    raw = json.dumps(doc).encode()
    path.write_bytes(raw)
    digest = hashlib.sha256(raw).hexdigest()
    monkeypatch.setenv("RECOVERY_KIS_QUOTA_SCOPE", "test")
    assert load_manifest(path, digest, "KIS", contract) == (
        doc["identity"],
        doc["policy"],
    )
    for wrong in ("b" * 64, ""):
        with pytest.raises(CoordinationError):
            load_manifest(path, wrong, "KIS", contract)
    with pytest.raises(CoordinationError):
        load_manifest(path, digest, "KIS", {**contract, "target": "asset:2"})
    monkeypatch.setenv("RECOVERY_KIS_QUOTA_SCOPE", "other")
    with pytest.raises(CoordinationError):
        load_manifest(path, digest, "KIS", contract)


@pytest.mark.parametrize(
    "module_name", ["workers.kis_price_collector", "workers.sec_financial_collector"]
)
def test_coordinated_dispatch_and_no_unfenced_failure(module_name):
    import importlib

    module = importlib.import_module(module_name)
    item = SimpleNamespace(item_id=1)
    args = SimpleNamespace(request_policy="coordinated-v1")
    db = Mock()
    with patch(
        "workers.coordinated_collector.process",
        side_effect=CoordinationError("lease_lost"),
    ) as process:
        with pytest.raises(CoordinationError):
            module.process_item(db, item, args)
        process.assert_called_once()
    with (
        patch("workers.coordinated_collector.claim", return_value=item),
        patch.object(
            module, "process_item", side_effect=CoordinationError("lease_lost")
        ),
        patch.object(module, "fail_item") as fail,
    ):
        with pytest.raises(CoordinationError):
            module.run_once(db, args)
        fail.assert_not_called()
