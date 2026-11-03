import pytest

from pipeline.durable_requests import CoordinationError
from workers.coordinated_consumer import recovery_topic


def test_recovery_topic_default_and_isolation(monkeypatch):
    monkeypatch.delenv("RECOVERY_RAW_TOPIC", raising=False)
    assert recovery_topic() == "trader.raw.coordinated.ready"
    monkeypatch.setenv("RECOVERY_RAW_TOPIC", "trader.raw.coordinated.ready649")
    assert recovery_topic() == "trader.raw.coordinated.ready649"


@pytest.mark.parametrize(
    "topic",
    [
        "",
        "trader.jobs.events649",
        "trader.raw.news.ready",
        "trader.raw.coordinated.ready650",
    ],
)
def test_unapproved_topics_rejected(monkeypatch, topic):
    monkeypatch.setenv("RECOVERY_RAW_TOPIC", topic)
    with pytest.raises(CoordinationError, match="invalid_contract"):
        recovery_topic()
