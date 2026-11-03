import json
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
import requests
from pipeline.request_policy import (
    Category,
    Failure,
    RequestExecutor,
    RequestGate,
    RequestPolicyError,
    RetryPolicy,
    classify,
    parse_retry_after,
)


class Clock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, duration):
        assert duration > 0
        self.sleeps.append(duration)
        self.now += duration


def response(status=200, body=None, retry_after=None):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(body if body is not None else {}).encode()
    result._content_consumed = True
    if retry_after is not None:
        result.headers["Retry-After"] = retry_after
    return result


def executor(clock=None, gate=None, **policy):
    clock = clock or Clock()
    return RequestExecutor(
        RetryPolicy(**policy),
        gate or RequestGate(1, clock=clock),
        clock=clock,
        sleep=clock.sleep,
        jitter=lambda: 1,
    ), clock


@pytest.mark.parametrize(
    "status,category",
    [
        (429, Category.RATE_LIMIT),
        (500, Category.TRANSIENT),
        (502, Category.TRANSIENT),
        (503, Category.TRANSIENT),
        (504, Category.TRANSIENT),
        (408, Category.TRANSIENT),
        (401, Category.AUTH),
        (403, Category.AUTH),
        (404, Category.NOT_FOUND),
        (400, Category.UNKNOWN),
        (501, Category.UNKNOWN),
    ],
)
def test_http_classification(status, category):
    assert classify(response=response(status)).category == category


@pytest.mark.parametrize(
    "error,category",
    [
        (requests.ConnectionError("SECRET"), Category.TRANSIENT),
        (requests.Timeout("SECRET"), Category.TRANSIENT),
        (requests.exceptions.ChunkedEncodingError("SECRET"), Category.TRANSIENT),
        (requests.exceptions.SSLError("SECRET"), Category.IMPLEMENTATION),
        (requests.exceptions.RetryError("SECRET"), Category.UNKNOWN),
        (FileNotFoundError("SECRET"), Category.IMPLEMENTATION),
    ],
)
def test_exception_classification_without_message_matching(error, category):
    result = classify(error=error)
    assert result.category == category
    assert "SECRET" not in str(result.metadata())


def test_provider_code_requires_provider_scope_and_sanitization():
    limited = response(200, {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "SECRET"})
    assert classify(response=limited, provider="KIS").category == Category.RATE_LIMIT
    assert classify(response=limited, provider="SEC") is None
    bad_code = response(500, {"msg_cd": "SECRET token=123", "token": "SECRET"})
    assert classify(response=bad_code, provider="KIS").provider_code is None
    assert (
        classify(error=RuntimeError("KIS daily price failed: EGW00201")).category
        == Category.UNKNOWN
    )


@pytest.mark.parametrize(
    "category",
    [
        Category.COMMIT,
        Category.QUALITY,
        Category.EMPTY,
        Category.PAUSED,
        Category.RESOURCE,
        Category.NO_CHANGE,
    ],
)
def test_explicit_domain_outcome_is_not_overruled_by_http(category):
    assert classify(response=response(503), outcome=category).category == category


def test_retry_after_seconds_date_and_large_wait_preserved():
    now = datetime(2026, 10, 22, tzinfo=timezone.utc)
    assert parse_retry_after("1000000", now) == (1000000, False)
    assert parse_retry_after("Thu, 22 Oct 2026 00:02:00 GMT", now) == (120, False)
    assert parse_retry_after("Wed, 21 Oct 2026 00:00:00 GMT", now) == (0, False)
    assert parse_retry_after(None) == (None, False)


@pytest.mark.parametrize("value", ["", "-1", "NaN", "1.5", "9" * 5000, "SECRET", 1])
def test_invalid_retry_after_is_not_silently_ignored(value):
    assert parse_retry_after(value) == (None, True)


def test_transport_retry_and_failed_request_pacing():
    engine, clock = executor()
    times = []

    def send(_):
        times.append(clock())
        if len(times) == 1:
            raise requests.ConnectionError("SECRET")
        return response()

    assert engine.request(send).status_code == 200
    assert times == [100, 101]
    assert (engine.requests, engine.retries) == (2, 1)
    assert "SECRET" not in str(engine.history)


def test_retry_credits_shared_across_pages_and_operation_limit():
    engine, _ = executor()
    for _ in range(2):
        send = Mock(side_effect=[response(503), response()])
        engine.request(send)
        assert send.call_count == 2
    send = Mock(return_value=response(503))
    with pytest.raises(RequestPolicyError) as error:
        engine.request(send)
    assert error.value.reason == "retry_budget"
    assert send.call_count == 1
    assert (engine.requests, engine.retries) == (5, 2)


def test_per_operation_attempts_and_total_request_cap():
    engine, _ = executor(max_attempts=2, max_retries=10)
    send = Mock(return_value=response(503))
    with pytest.raises(RequestPolicyError):
        engine.request(send)
    assert send.call_count == 2
    engine, _ = executor(max_requests=1)
    engine.request(lambda _: response())
    send = Mock()
    with pytest.raises(RequestPolicyError, match="request_budget"):
        engine.request(send)
    send.assert_not_called()


def test_rate_limit_propagates_to_another_client_even_without_retry():
    clock = Clock()
    gate = RequestGate(1, clock=clock)
    first, _ = executor(clock, gate, max_retries=0)
    with pytest.raises(RequestPolicyError):
        first.request(lambda _: response(429, retry_after="90"))
    second, _ = executor(clock, gate)
    times = []
    second.request(lambda _: times.append(clock()) or response())
    assert times == [190]


def test_business_rate_limit_uses_same_budget():
    engine, clock = executor()
    send = Mock(
        side_effect=[response(200, {"rt_cd": "1", "msg_cd": "EGW00201"}), response()]
    )
    engine.request(send, provider="KIS")
    assert send.call_count == 2 and clock() == 160


def test_provider_wait_over_budget_is_not_clipped():
    engine, clock = executor(max_elapsed=100)
    send = Mock(return_value=response(429, retry_after="1000000"))
    with pytest.raises(RequestPolicyError, match="wait_exceeds_budget"):
        engine.request(send)
    assert send.call_count == 1
    assert clock.sleeps == []
    assert engine.gate.not_before == 1000100


def test_invalid_provider_wait_blocks_shared_gate():
    engine, _ = executor()
    with pytest.raises(RequestPolicyError, match="invalid_retry_after"):
        engine.request(lambda _: response(429, retry_after="SECRET"))
    send = Mock()
    with pytest.raises(RequestPolicyError, match="quota_review_required"):
        engine.request(send)
    send.assert_not_called()


@pytest.mark.parametrize("status", [401, 403, 404, 400, 501])
def test_permanent_response_is_not_retried(status):
    engine, _ = executor()
    send = Mock(return_value=response(status))
    with pytest.raises(RequestPolicyError) as error:
        engine.request(send)
    assert send.call_count == 1
    assert error.value.response.status_code == status


def test_token_post_counted_but_not_retried():
    engine, _ = executor()
    send = Mock(return_value=response(503))
    with pytest.raises(RequestPolicyError, match="non_idempotent_method"):
        engine.request(send, method="POST")
    assert send.call_count == 1 and engine.requests == 1 and engine.retries == 0


def test_cancellation_during_wait_prevents_next_request():
    engine, clock = executor()
    engine.stopped = lambda: clock() >= 102
    send = Mock(return_value=response(429))
    with pytest.raises(RequestPolicyError) as error:
        engine.request(send)
    assert error.value.failure.category == Category.PAUSED
    assert send.call_count == 1


def test_no_http_retry_for_commit_or_quality_exception():
    for category in (Category.COMMIT, Category.QUALITY, Category.EMPTY):
        engine, _ = executor()
        send = Mock(side_effect=RequestPolicyError(Failure(category)))
        with pytest.raises(RequestPolicyError) as error:
            engine.request(send)
        assert error.value.failure.category == category
        assert send.call_count == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"max_retries": -1},
        {"max_elapsed": float("inf")},
        {"base_delay": 0},
        {"max_requests": True},
        {"max_delay": 0.5},
    ],
)
def test_invalid_policy_rejected(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


def test_elapsed_budget_prevents_send_and_limits_timeout_argument():
    engine, clock = executor(max_elapsed=10)
    clock.now += 9
    timeouts = []
    engine.request(lambda remaining: timeouts.append(remaining) or response())
    assert timeouts == [1]
    clock.now += 1
    send = Mock()
    with pytest.raises(RequestPolicyError, match="elapsed_budget"):
        engine.request(send)
    send.assert_not_called()


def test_verified_no_change_and_empty_require_explicit_evidence():
    assert classify(response=response(200, {"output2": []}), provider="KIS") is None
    assert classify(outcome=Category.EMPTY).category != Category.NO_CHANGE


def test_legacy_and_common_http_facts_agree_without_modifying_legacy():
    from collectors.kis_diagnostics import error_metadata

    for status, code in [(429, None), (500, "EGW00201"), (503, None), (404, None)]:
        raw = response(status, {"msg_cd": code}, "60")
        error = requests.HTTPError("SECRET", response=raw)
        old = error_metadata(error)
        new = classify(error=error, provider="KIS")
        assert old["httpStatus"] == new.http_status
        assert old["providerErrorCode"] == new.provider_code
        assert old["retryAfterSeconds"] == new.retry_after
        if old["failureCategory"] == "RATE_LIMIT":
            assert new.category == Category.RATE_LIMIT
        elif old["failureCategory"] == "PROVIDER_5XX":
            assert new.category == Category.TRANSIENT
