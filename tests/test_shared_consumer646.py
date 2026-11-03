import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from workers import kafka_pipeline_consumer as worker


def test_shutdown_finishes_current_and_does_not_commit_future_record(monkeypatch):
    stop = threading.Event()
    consumer, connection, heartbeat = MagicMock(), MagicMock(), MagicMock()
    first = SimpleNamespace(topic="fixture", partition=0, offset=3)
    future = SimpleNamespace(topic="fixture", partition=0, offset=4)
    consumer.poll.return_value = {"partition": [first, future]}
    def handle(*args):
        stop.set()
        return "success"
    handler = MagicMock(side_effect=handle)
    monkeypatch.setattr(worker, "process_message", handler)
    worker.consume(consumer, connection, heartbeat, SimpleNamespace(consumer_name="test", max_records=0), stop)
    handler.assert_called_once_with(connection, first, "test")
    committed = list(consumer.commit.call_args.args[0].values())
    assert committed[0].offset == 4


def test_unresolved_processing_never_commits(monkeypatch):
    stop = threading.Event()
    consumer = MagicMock()
    consumer.poll.return_value = {"partition": [SimpleNamespace(topic="fixture", partition=0, offset=0)]}
    monkeypatch.setattr(worker, "process_message", MagicMock(side_effect=RuntimeError("unresolved")))
    with pytest.raises(RuntimeError):
        worker.consume(consumer, MagicMock(), MagicMock(), SimpleNamespace(consumer_name="test", max_records=1), stop)
    consumer.commit.assert_not_called()


def test_no_admission_after_stop(monkeypatch):
    stop = threading.Event()
    stop.set()
    consumer = MagicMock()
    worker.consume(consumer, MagicMock(), MagicMock(), SimpleNamespace(consumer_name="test", max_records=0), stop)
    consumer.poll.assert_not_called()


@pytest.mark.parametrize("kind", ["storage", "sql"])
def test_uncertain_store_or_sql_is_not_marked_processed(monkeypatch, kind):
    from collectors.s3_raw import RawStorageError
    import psycopg2
    error = RawStorageError("unconfirmed") if kind == "storage" else psycopg2.OperationalError("unconfirmed")
    monkeypatch.setattr(worker, "find_processed_event", lambda *args: None)
    monkeypatch.setattr(worker, "claim_queued_item_by_id", lambda *args, **kwargs: SimpleNamespace(item_id=1))
    monkeypatch.setattr(worker, "dispatch_item", MagicMock(side_effect=error))
    recorded, failed = MagicMock(), MagicMock()
    monkeypatch.setattr(worker, "record_processed_event", recorded)
    monkeypatch.setattr(worker, "fail_claimed_item", failed)
    message = SimpleNamespace(value={"idempotencyKey":"fixture", "itemId":1, "source":"BLS", "jobType":"TEST"})
    with pytest.raises(type(error)):
        worker.process_message(MagicMock(), message, "fixture")
    recorded.assert_not_called()
    failed.assert_not_called()


def test_preview_cleanup_error_does_not_overwrite_committed_success(monkeypatch):
    import collectors.common
    monkeypatch.setenv("RAW_STORAGE_BACKEND", "S3")
    monkeypatch.setattr(worker, "find_processed_event", lambda *args: None)
    monkeypatch.setattr(worker, "claim_queued_item_by_id", lambda *args, **kw: SimpleNamespace(item_id=1,job_id=2))
    monkeypatch.setattr(worker, "dispatch_item", lambda *args: None)
    monkeypatch.setattr(worker, "current_event_status", lambda *args: ("SUCCESS",None))
    monkeypatch.setattr(collectors.common, "cleanup_verified_s3_previews", MagicMock(side_effect=RuntimeError("cleanup")))
    recorded, failed=MagicMock(),MagicMock()
    monkeypatch.setattr(worker,"record_processed_event",recorded)
    monkeypatch.setattr(worker,"fail_claimed_item",failed)
    message=SimpleNamespace(value={"idempotencyKey":"fixture","itemId":1,"source":"BLS","jobType":"TEST"},topic="fixture",partition=0,offset=0)
    with pytest.raises(RuntimeError,match="cleanup"):
        worker.process_message(MagicMock(),message,"fixture")
    recorded.assert_called_once()
    failed.assert_not_called()
