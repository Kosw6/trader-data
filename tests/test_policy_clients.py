import json
import threading
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import pytest
import requests
from collectors.kis_client import KISClient
from collectors.policy_clients import (
    PolicyKISClient,
    PolicySECClient,
    PolicySession,
    kis_client,
    policy_checkpoint,
)
from pipeline.request_policy import Category, RequestPolicyError

from tests.test_request_policy import executor, response


def test_legacy_factory_is_unchanged_and_default_off(monkeypatch):
    monkeypatch.setenv("APPKEY", "dummy")
    monkeypatch.setenv("APPSECRET", "dummy")
    client = kis_client()
    assert type(client) is KISClient
    assert client.session.adapters["https://"].max_retries.total == 4
    client.session.close()
    with pytest.raises(ValueError):
        kis_client("typo")


@pytest.mark.parametrize("provider", ["KIS", "SEC"])
def test_no_hidden_adapter_retries(provider):
    engine, _ = executor()
    with PolicySession(provider, engine, "test") as session:
        for adapter in session.adapters.values():
            retry = adapter.max_retries
            assert (
                retry.total
                == retry.connect
                == retry.read
                == retry.status
                == retry.redirect
                == 0
            )
            assert not retry.raise_on_status


def test_kis_business_retry_empty_and_raw_preservation():
    engine, _ = executor()
    client = PolicyKISClient("dummy", "dummy", access_token="dummy", executor=engine)
    empty = {"rt_cd": "0", "output2": []}
    with patch.object(
        requests.Session,
        "request",
        side_effect=[
            response(200, {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "SECRET"}),
            response(200, empty),
        ],
    ) as send:
        result = client.get_daily_price_page("NAS", "TEST", date(2026, 1, 1))
    assert result["bars"] == [] and result["raw_payload"] == empty
    assert send.call_count == 2
    receipt = policy_checkpoint(client)
    assert receipt["requestPolicy"]["result"]["failureCategory"] == "EMPTY_UNRESOLVED"
    assert "SECRET" not in str(receipt)
    client.session.close()


def test_token_and_page_share_request_budget_without_token_leak():
    engine, _ = executor(max_requests=1)
    client = PolicyKISClient("dummy", "dummy", executor=engine)
    with (
        patch.object(
            requests.Session,
            "request",
            return_value=response(200, {"access_token": "SECRET"}),
        ) as send,
        pytest.raises(RequestPolicyError, match="request_budget") as error,
    ):
        client.get_daily_price_page("NAS", "TEST", date(2026, 1, 1))
    assert send.call_count == 1
    assert "SECRET" not in str(error.value.metadata())
    client.session.close()


def test_missing_token_does_not_expose_payload_or_retry():
    engine, _ = executor()
    client = PolicyKISClient("dummy", "dummy", executor=engine)
    with (
        patch.object(
            requests.Session,
            "request",
            return_value=response(200, {"secret": "SECRET"}),
        ) as send,
        pytest.raises(RequestPolicyError, match="token_missing") as error,
    ):
        client.get_access_token()
    assert send.call_count == 1 and "SECRET" not in str(error.value)
    client.session.close()


def test_sec_404_remains_status_compatible_and_non_retryable():
    engine, _ = executor()
    client = PolicySECClient("test", executor=engine)
    with (
        patch.object(requests.Session, "request", return_value=response(404)) as send,
        pytest.raises(RequestPolicyError) as error,
    ):
        client.get_company_facts("0000000001")
    assert error.value.response.status_code == 404
    assert error.value.failure.category == Category.NOT_FOUND
    assert send.call_count == 1
    client.session.close()


def test_invalid_json_does_not_retry_or_leak_body():
    engine, _ = executor()
    client = PolicySECClient("test", executor=engine)
    invalid = response()
    invalid._content = b"SECRET not json"
    with (
        patch.object(requests.Session, "request", return_value=invalid) as send,
        pytest.raises(RequestPolicyError, match="invalid_json_contract") as error,
    ):
        client.get_company_facts("0000000001")
    assert "SECRET" not in str(error.value)
    assert send.call_count == 1
    client.session.close()


def test_worker_cli_opt_in_and_defaults(monkeypatch):
    from workers import kis_price_collector, sec_financial_collector

    for worker in (kis_price_collector, sec_financial_collector):
        monkeypatch.setattr("sys.argv", ["worker"])
        assert worker.parse_args().request_policy == "legacy"
        monkeypatch.setattr("sys.argv", ["worker", "--request-policy", "bounded-v1"])
        assert worker.parse_args().request_policy == "bounded-v1"


@pytest.mark.parametrize(
    "worker_name", ["kis_price_collector", "sec_financial_collector"]
)
def test_worker_persists_allowlisted_failure_checkpoint(worker_name):
    from importlib import import_module
    from types import SimpleNamespace
    from unittest.mock import Mock

    from pipeline.request_policy import Failure

    worker = import_module(f"workers.{worker_name}")
    item = SimpleNamespace(item_id=1, job_id=2, item_key="TEST", parameters={})
    failure = RequestPolicyError(
        Failure(Category.RATE_LIMIT, 429, retry_after=60),
        "retry_budget",
        attempts=3,
        retries=2,
    )
    connection = Mock()
    with (
        patch.object(worker, "claim_next_queued_item", return_value=item),
        patch.object(worker, "process_item", side_effect=failure) as process,
        patch.object(worker, "fail_item", return_value="FAILED") as failed,
        pytest.raises(RequestPolicyError),
    ):
        worker.run_once(connection, SimpleNamespace(job_type="TEST"))
    connection.rollback.assert_called_once()
    process.assert_called_once()
    metadata = failed.call_args.kwargs["checkpoint"]["requestPolicy"]
    assert metadata["failureCategory"] == "RATE_LIMIT"
    assert metadata["requestAttempts"] == 3
    assert metadata["retriesUsed"] == 2


def test_shared_provider_gate_cannot_be_accelerated(monkeypatch):
    from collectors import policy_clients

    monkeypatch.setattr(policy_clients, "_GATES", {})
    first = policy_clients.provider_gate("KIS", 2)
    first.not_before = 12345
    second = policy_clients.provider_gate("KIS", 0.1)
    assert first is second
    assert second.interval == 2 and second.not_before == 12345
    assert policy_clients.provider_gate("SEC", 1) is not first


def test_price_payload_parsing_matches_legacy_and_does_not_fill_ohlc():
    from collectors.kis_client import normalize_daily_price

    engine, _ = executor()
    client = PolicyKISClient("dummy", "dummy", access_token="dummy", executor=engine)
    raw = {
        "xymd": "20260101",
        "open": "",
        "high": "",
        "low": "",
        "clos": "123.45",
        "tvol": "0",
    }
    with patch.object(
        requests.Session, "request", return_value=response(200, {"output2": [raw]})
    ):
        result = client.get_daily_price_page("NAS", "TEST", date(2026, 1, 1))
    assert result["bars"] == [normalize_daily_price(raw)]
    assert result["bars"][0]["open"] is None
    client.session.close()


@pytest.mark.parametrize("first_status,expected_calls", [(503, 2), (403, 1), (302, 1)])
def test_real_requests_stack_no_hidden_retries_or_redirects(
    first_status, expected_calls
):
    # Loopback only. Exercise urllib3, not just a mocked Session.
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.path)
            self.send_response(first_status if len(calls) == 1 else 200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Location", "/must-not-follow")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True}).encode())

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    engine, _ = executor()
    try:
        with PolicySession("SEC", engine, "test") as session:
            session.trust_env = False
            url = f"http://127.0.0.1:{server.server_port}/test"
            if first_status == 503:
                assert session.get(url).json() == {"ok": True}
            else:
                with pytest.raises(RequestPolicyError):
                    session.get(url)
        assert calls == ["/test"] * expected_calls
        assert engine.requests == expected_calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
