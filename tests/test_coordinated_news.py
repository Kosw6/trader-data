import copy
import hashlib
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from collectors.news_http import NewsFetchError, NewsHTTP
from pipeline.coordinated_news import (
    VERSION,
    collection_contract,
    decode,
    envelope,
    normalized,
)
from pipeline.durable_requests import Controller, CoordinationError
from tests.test_news_collection import gkg, item, rss
from workers import coordinated_consumer
from workers.coordinated_news_collector import DurableNewsHTTP, durable_storage
from workers.coordinated_news_consumer import news_topic, trusted_plan


def contract(provider="RSS"):
    i = item(provider)
    i.parameters["recoveryVersion"] = VERSION
    return collection_contract(i)


@pytest.mark.parametrize("provider", ["RSS", "GDELT"])
def test_exact_raw_replay_preserves_discovery_boundaries(provider):
    c = contract(provider)
    raw = rss(2) if provider == "RSS" else gkg()
    e = envelope(raw, c, {"http": 200})
    assert decode(e, c) == raw
    a = normalized(e, c)
    assert a == normalized(copy.deepcopy(e), copy.deepcopy(c))
    assert a[0]["stats"]["invalid"] == 0 and a[0]["observations"]
    for r in a[0]["observations"]:
        m = r["metadata"]
        assert (
            m["ragEligible"] is False
            and m["body"] is None
            and m["originalArticleVerified"] is False
        )
        assert m["collectionContractHash"] and r["observationHash"]


@pytest.mark.parametrize(
    "field,value",
    [("rawSha256", "0" * 64), ("rawBytes", 0), ("rawBase64", "!"), ("version", "old")],
)
def test_altered_raw_or_contract_is_rejected(field, value):
    c = contract()
    e = envelope(rss(), c, {"http": 200})
    e[field] = value
    with pytest.raises(CoordinationError):
        decode(e, c)


def test_contract_binds_query_day_and_parameters():
    c = contract()
    other = copy.deepcopy(c)
    other["unit"] = "2016-12-29"
    with pytest.raises(CoordinationError):
        normalized(envelope(rss(), c, {}), other)
    assert normalized(envelope(rss(0), c, {}), c)[0]["observations"] == []


def test_bad_gdelt_rows_are_preserved_as_review_not_silently_success():
    c = contract("GDELT")
    a = normalized(envelope(gkg(bad=True), c, {}), c)
    assert a[0]["stats"]["invalid"] > 0


def test_news_http_reports_complete_response_once(monkeypatch):
    lease = Mock()
    lease.reserve.return_value = "request"
    monkeypatch.setattr(NewsHTTP, "fetch", lambda *_: (b"raw", {"http": 200}))
    c = DurableNewsHTTP(lease)
    try:
        assert c.fetch("RSS", "2016-12-30", "NVIDIA") == (b"raw", {"http": 200})
    finally:
        c.close()
    lease.reserve.assert_called_once()
    lease.report.assert_called_once_with("request", "HTTP_OK")


def test_persistent_budget_limits_transient_news_retries(monkeypatch):
    lease = Mock()
    lease.reserve.side_effect = ["one", CoordinationError("budget_exhausted")]

    def unavailable(*_):
        raise NewsFetchError("NEWS_HTTP_503", True)

    monkeypatch.setattr(NewsHTTP, "fetch", unavailable)
    c = DurableNewsHTTP(lease)
    try:
        with pytest.raises(CoordinationError, match="budget_exhausted"):
            c.fetch("RSS", "2016-12-30", "NVIDIA")
    finally:
        c.close()
    lease.report.assert_called_once_with("one", "TRANSIENT_TRANSPORT", 0)


def test_unknown_transport_outcome_stays_pending(monkeypatch):
    lease = Mock()

    def crash(*_):
        raise SystemExit()

    monkeypatch.setattr(NewsHTTP, "fetch", crash)
    c = DurableNewsHTTP(lease)
    try:
        with pytest.raises(SystemExit):
            c.fetch("RSS", "2016-12-30", "NVIDIA")
    finally:
        c.close()
    lease.reserve.assert_called_once()
    lease.report.assert_not_called()


def test_news_topic_isolated_and_unknown_plan_rejected(monkeypatch):
    monkeypatch.setenv("NEWS_RECOVERY_TOPIC", "trader.raw.coordinated.news.ready649")
    assert news_topic().endswith("news.ready649")
    monkeypatch.setenv("NEWS_RECOVERY_TOPIC", "trader.jobs.events649")
    with pytest.raises(CoordinationError):
        news_topic()
    with pytest.raises(CoordinationError):
        trusted_plan(Mock(), "bad/key")


def test_control_proxy_is_explicit_and_bounded():
    api = Controller("https://example.com", "fixture", proxy_prefix="/api/controller")
    assert (
        api.url
        == "https://example.com/api/controller/api/v1/admin/pipeline/recovery/work"
    )
    api.close()
    with pytest.raises(CoordinationError):
        Controller("https://example.com", "fixture", proxy_prefix="/other")


def test_offset_is_not_committed_when_db_validation_fails(monkeypatch):
    import json

    from kafka import TopicPartition

    key = hashlib.sha256(b"unit").hexdigest()
    topic = "trader.raw.coordinated.news.ready649"
    message = SimpleNamespace(
        topic=topic,
        key=key.encode(),
        value=json.dumps({"workKey": key}).encode(),
        offset=4,
        partition=0,
    )
    consumer = Mock()
    consumer.poll.return_value = {TopicPartition(topic, 0): [message]}

    def fail(*_, **__):
        raise CoordinationError("contract_conflict")

    monkeypatch.setattr(coordinated_consumer, "process_event", fail)
    with pytest.raises(CoordinationError):
        coordinated_consumer.consume_once(
            consumer, Mock(), Mock(), {key: {}}, ".", topic=topic
        )
    consumer.commit.assert_not_called()


def test_shared_legacy_pacing_commits_before_http(monkeypatch):
    order = []
    from unittest.mock import MagicMock

    db = MagicMock()
    db.cursor.return_value.__enter__.return_value.fetchone.return_value = (0,)
    db.commit.side_effect = lambda: order.append("pacing-commit")
    lease = Mock()
    lease.reserve.side_effect = lambda *_: order.append("durable-reservation") or "one"

    def fetch(*_):
        order.append("http-send")
        return b"raw", {"http": 200}

    monkeypatch.setattr(NewsHTTP, "fetch", fetch)
    client = DurableNewsHTTP(lease, connection=db)
    try:
        client.fetch("RSS", "2016-12-30", "NVIDIA")
    finally:
        client.close()
    assert order == ["durable-reservation", "pacing-commit", "http-send"]


def test_news_durable_storage_does_not_change_shared_s3_environment(
    monkeypatch, tmp_path
):
    import os

    monkeypatch.setenv("RAW_STORAGE_BACKEND", "S3")
    monkeypatch.setenv("RAW_STORAGE_BASE_URI", "s3://fixture/old")
    storage = durable_storage(tmp_path, 123)
    assert storage.storage_backend == "LOCAL" and storage.storage_base_uri == ""
    assert (
        os.environ["RAW_STORAGE_BACKEND"] == "S3"
        and os.environ["RAW_STORAGE_BASE_URI"] == "s3://fixture/old"
    )


def test_command_collector_leaves_dispatch_to_recovery_consumer(monkeypatch):
    from workers import coordinated_news_collector as module

    monkeypatch.setenv("NEWS_RECOVERY_ENABLED", "true")
    monkeypatch.setenv("NEWS_RECOVERY_RAW_ROOT", "/fixture")
    raw = Mock(return_value={"workKey": "a" * 64})
    monkeypatch.setattr(module, "collect_raw", raw)
    api = Mock()
    monkeypatch.setattr(module, "controller", api)
    db = Mock()
    i = Mock()
    assert module.process_item(db, i) == {"workKey": "a" * 64}
    raw.assert_called_once_with(db, i, "/fixture")
    api.assert_not_called()


def test_duplicate_complete_news_requires_independent_actual_value_verification(
    monkeypatch,
):
    import json
    from unittest.mock import MagicMock

    from kafka import TopicPartition

    from pipeline import coordinated_news, db_connections
    from workers import coordinated_scheduler

    key = "c" * 64
    topic = "trader.raw.coordinated.news.ready649"
    payload = {"eventVersion": 2, "eventType": "COORDINATED_RAW_READY", "workKey": key}
    db = MagicMock()
    db.cursor.return_value.__enter__.return_value.fetchone.side_effect = [
        (payload,),
        ("COMPLETE", "COMPLETE", "hash", "hash"),
    ]
    monkeypatch.setattr(coordinated_consumer, "advance", Mock(return_value=[]))
    monkeypatch.setattr(
        coordinated_scheduler, "audit", Mock(return_value={"unresolvedTotal": 0})
    )
    read = MagicMock()
    monkeypatch.setattr(db_connections, "independent_connection", lambda _: read)
    verifier = Mock(side_effect=CoordinationError("contract_conflict"))
    monkeypatch.setattr(
        coordinated_news, "news_publication_verifier", lambda _: verifier
    )
    message = SimpleNamespace(
        topic=topic,
        key=key.encode(),
        value=json.dumps(payload).encode(),
        offset=7,
        partition=0,
    )
    consumer = Mock()
    consumer.poll.return_value = {TopicPartition(topic, 0): [message]}
    with pytest.raises(CoordinationError, match="contract_conflict"):
        coordinated_consumer.consume_once(
            consumer,
            db,
            Mock(),
            {key: {"kind": "NEWS", "workKey": key}},
            ".",
            topic=topic,
        )
    verifier.assert_called_once()
    consumer.commit.assert_not_called()
