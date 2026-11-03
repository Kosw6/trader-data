"""Only run by Go's two-controller harness against its throwaway schema."""

import hashlib
import json
import os
import re
import threading
import time
import uuid
from contextlib import closing
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import psycopg2
import pytest
from collectors.common import LocalPipelineStorage
from pipeline.durable_raw import JobGuard, commit_raw
from pipeline.durable_requests import (
    Controller,
    CoordinationError,
    DurableLease,
    DurableSession,
    canonical_hash,
)


@pytest.fixture
def db():
    dsn = os.getenv("RECOVERY_TEST_DATABASE_URL", "")
    schema = os.getenv("RECOVERY_TEST_SCHEMA", "")
    if not os.getenv("RECOVERY_TEST_URL"):
        pytest.skip("Go bridge not configured")
    params = psycopg2.extensions.parse_dsn(dsn)
    assert (
        params["dbname"] == "recovery451_test"
        and params["host"] in ("localhost", "127.0.0.1")
        and params["port"] == "55451"
    )
    assert re.fullmatch(r"coord_[0-9]+", schema)
    connection = psycopg2.connect(dsn, options=f"-c search_path={schema}")
    with connection.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS source_object(
          id bigserial PRIMARY KEY,provider text,data_type text,external_id text,pipeline_job_item_id bigint,
          storage_backend text,storage_key text,object_uri text,object_size bigint,content_hash text,
          dedupe_key text UNIQUE,collected_at timestamptz,processing_status text);
          CREATE TABLE IF NOT EXISTS pipeline_job(id bigint PRIMARY KEY,status text,started_at timestamptz,completed_at timestamptz,updated_at timestamptz);
          CREATE TABLE IF NOT EXISTS pipeline_job_item(id bigint PRIMARY KEY,job_id bigint,status text,attempt_count integer,
          checkpoint jsonb,error_code text,error_message text,completed_at timestamptz);""")
        cur.execute("""CREATE TABLE IF NOT EXISTS pipeline_outbox(
          id bigserial PRIMARY KEY,source_object_id bigint,job_item_id bigint,event_type text,topic text,message_key text,
          idempotency_key text UNIQUE,payload jsonb,status text,attempt_count integer,next_attempt_at timestamptz);
          SELECT to_regclass('pipeline_recovery_raw_intent');""")
        if cur.fetchone()[0] is None:
            cur.execute(
                (
                    Path(__file__).resolve().parents[2]
                    / "trader-controller/db/migration/V044__coordinated_raw_intent.sql"
                ).read_text()
            )
    connection.commit()
    yield connection
    connection.close()


def policy(**kwargs):
    return {
        "version": "coord-v1",
        "maxClaims": 20,
        "maxRequests": 8,
        "maxRetries": 2,
        "maxElapsedMs": 120000,
        "leaseMs": 30000,
        "intervalMs": 0,
        "baseDelayMs": 1,
        "maxDelayMs": 10,
        "rateLimitMs": 10,
        **kwargs,
    }


def lease_for(*, url=None, p=None, identity=None):
    identity = identity or {
        "provider": "SEC",
        "quotaScope": uuid.uuid4().hex,
        "target": "company:1",
        "dataset": "sec-all-history",
        "start": "2026-01-01",
        "end": "2026-01-02",
        "revision": canonical_hash(uuid.uuid4().hex),
    }
    return DurableLease(
        Controller(url or os.environ["RECOVERY_TEST_URL"], "test-only"),
        identity,
        p or policy(),
    )


def expire(db, lease):
    with db.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_recovery_work SET lease_until=clock_timestamp()-interval '1 second' WHERE work_key=%s",
            (lease.key,),
        )
    db.commit()


@pytest.fixture
def provider():
    state = {"calls": [], "responses": []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state["calls"].append(self.path)
            status, body = (
                state["responses"].pop(0) if state["responses"] else (200, {})
            )
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", state
    server.shutdown()
    server.server_close()
    thread.join()


def source(tmp_path):
    store = LocalPipelineStorage(tmp_path)
    store.storage_backend = "LOCAL"
    store.storage_base_uri = ""
    return store.write_raw_json(
        "SEC",
        "SUBMISSIONS",
        uuid.uuid4().hex,
        {"verified": "raw"},
        datetime.now(timezone.utc),
    )


def test_two_controllers_and_durable_http_budget(db, provider):
    url, state = provider
    state["responses"] = [(503, {}), (200, {"ok": True})]
    lease = lease_for(p=policy(maxRequests=2))
    try:
        with DurableSession("SEC", lease, "test", allowed_origins={url}) as client:
            assert client.get(url + "/page").json() == {"ok": True}
        work = lease.api.call("GET", f"/{lease.key}")
        assert (
            work["requests"] == 2 and work["retries"] == 1 and len(state["calls"]) == 2
        )
        identity, p = lease.identity, lease.policy
    finally:
        lease.close()
    expire(db, lease)
    new = lease_for(url=os.environ["RECOVERY_TEST_URL_B"], p=p, identity=identity)
    try:
        with (
            DurableSession("SEC", new, "test", allowed_origins={url}) as client,
            pytest.raises(CoordinationError, match="budget_exhausted"),
        ):
            client.get(url + "/different-page")
        assert len(state["calls"]) == 2
    finally:
        new.close()


def test_pending_is_not_refunded_after_worker_loss(db):
    lease = lease_for()
    lease.reserve("page1")
    lease.close()
    expire(db, lease)
    api = Controller(os.environ["RECOVERY_TEST_URL_B"], "test-only")
    try:
        with pytest.raises(CoordinationError, match="review_required"):
            DurableLease(api, lease.identity, lease.policy)
        assert api.call("GET", f"/{lease.key}")["requests"] == 1
    finally:
        api.close()


def test_reservation_response_lost_never_sends(db, provider):
    url, state = provider
    lease = lease_for()
    original = lease.api.call

    def lost(method, path="", body=None):
        result = original(method, path, body)
        if path.endswith("/requests"):
            raise CoordinationError("commit_uncertain")
        return result

    try:
        with (
            patch.object(lease.api, "call", lost),
            DurableSession("SEC", lease, "test", allowed_origins={url}) as client,
            pytest.raises(CoordinationError, match="commit_uncertain"),
        ):
            client.get(url + "/never-sent")
        assert state["calls"] == []
        assert original("GET", f"/{lease.key}")["requests"] == 1
    finally:
        lease.close()


def test_report_response_lost_never_replays(db, provider):
    url, state = provider
    state["responses"] = [(503, {})]
    lease = lease_for()
    original = lease.api.call

    def lost(method, path="", body=None):
        result = original(method, path, body)
        if path.endswith("/report"):
            raise CoordinationError("commit_uncertain")
        return result

    try:
        with (
            patch.object(lease.api, "call", lost),
            DurableSession("SEC", lease, "test", allowed_origins={url}) as client,
            pytest.raises(CoordinationError, match="commit_uncertain"),
        ):
            client.get(url + "/once")
        assert state["calls"] == ["/once"]
    finally:
        lease.close()


def test_existing_raw_metadata_is_not_overwritten(db, tmp_path):
    from pipeline.durable_raw import insert_raw

    raw = source(tmp_path)
    with db.cursor() as cur:
        original_id = insert_raw(cur, raw)
        cur.execute(
            "UPDATE source_object SET processing_status='PROCESSED' WHERE id=%s",
            (original_id,),
        )
    db.commit()
    lease = lease_for()
    try:
        result = commit_raw(db, lease, [raw], tmp_path)
        assert result["sourceIds"] == [original_id]
        with db.cursor() as cur:
            cur.execute(
                "SELECT processing_status,object_uri FROM source_object WHERE id=%s",
                (original_id,),
            )
            assert cur.fetchone() == ("PROCESSED", raw["object_uri"])
        db.rollback()
    finally:
        lease.close()


def test_raw_hash_conflict_rolls_back_receipt(db, tmp_path):
    from pipeline.durable_raw import insert_raw

    raw = source(tmp_path)
    with db.cursor() as cur:
        original_id = insert_raw(cur, raw)
        cur.execute(
            "UPDATE source_object SET content_hash=%s WHERE id=%s",
            ("b" * 64, original_id),
        )
    db.commit()
    lease = lease_for()
    try:
        with pytest.raises(CoordinationError, match="review_required"):
            commit_raw(db, lease, [raw], tmp_path)
        with db.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pipeline_recovery_receipt WHERE work_key=%s",
                (lease.key,),
            )
            assert cur.fetchone()[0] == 0
        db.rollback()
    finally:
        lease.close()


def test_raw_commit_receipt_atomic_and_replay_blocked(db, tmp_path):
    lease = lease_for()
    raw = source(tmp_path)
    try:
        result = commit_raw(db, lease, [raw], tmp_path)
        receipt = lease.api.call("GET", f"/{lease.key}/receipts/{result['receiptId']}")
        assert receipt["hash"] == result["hash"]
        assert lease.api.call("GET", f"/{lease.key}")["phase"] == "RAW_READY"
    finally:
        lease.close()
    api = Controller(os.environ["RECOVERY_TEST_URL_B"], "test-only")
    try:
        with pytest.raises(CoordinationError, match="already_committed"):
            DurableLease(api, lease.identity, lease.policy)
    finally:
        api.close()


def test_expiry_during_raw_write_rolls_back(db, tmp_path):
    from pipeline import durable_raw

    lease = lease_for(p=policy(leaseMs=1000))
    raw = source(tmp_path)
    original = durable_raw.insert_raw
    called = []

    def slow(cursor, source):
        result = original(cursor, source)
        called.append(True)
        time.sleep(1.2)
        return result

    try:
        with (
            patch.object(durable_raw, "insert_raw", slow),
            pytest.raises(CoordinationError, match="lease_lost"),
        ):
            commit_raw(db, lease, [raw], tmp_path)
        assert called == [True]
        with db.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM source_object WHERE dedupe_key=%s",
                (raw["dedupe_key"],),
            )
            assert cur.fetchone()[0] == 0
            cur.execute(
                "SELECT count(*) FROM pipeline_recovery_receipt WHERE work_key=%s",
                (lease.key,),
            )
            assert cur.fetchone()[0] == 0
        db.rollback()
    finally:
        lease.close()


def test_commit_response_lost_requires_independent_read(db, tmp_path):
    lease = lease_for()
    raw = source(tmp_path)

    class LostReply:
        def __getattr__(self, name):
            return getattr(db, name)

        def commit(self):
            db.commit()
            raise psycopg2.OperationalError("injected response loss")

    try:
        with pytest.raises(CoordinationError, match="commit_uncertain"):
            commit_raw(LostReply(), lease, [raw], tmp_path)
        work = lease.api.call("GET", f"/{lease.key}")
        assert work["phase"] == "RAW_READY"
        digest = canonical_hash(
            [{k: raw[k] for k in ("dedupe_key", "content_hash", "object_size")}]
        )
        assert (
            lease.api.call("GET", f"/{lease.key}/receipts/raw-{digest}")["hash"]
            == digest
        )
    finally:
        lease.close()


def test_changed_raw_rejected_before_sql(db, tmp_path):
    lease = lease_for()
    raw = source(tmp_path)
    raw["content_hash"] = "0" * 64
    try:
        with pytest.raises(CoordinationError, match="review_required"):
            commit_raw(db, lease, [raw], tmp_path)
    finally:
        lease.close()


def make_item(db):
    item = SimpleNamespace(
        item_id=int(uuid.uuid4().hex[:10], 16),
        job_id=int(uuid.uuid4().hex[:10], 16),
        attempt_count=1,
        item_key="KIS:1",
        parameters={},
    )
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_job(id,status) VALUES(%s,'RUNNING')", (item.job_id,)
        )
        cur.execute(
            "INSERT INTO pipeline_job_item(id,job_id,status,attempt_count) VALUES(%s,%s,'RUNNING',1)",
            (item.item_id, item.job_id),
        )
    db.commit()
    return item


def ready_intent(db, tmp_path):
    item = make_item(db)
    lease = lease_for()
    raw = source(tmp_path)
    result = commit_raw(
        db, lease, [raw], tmp_path, item, {"target": lease.identity["target"]}
    )
    lease.close()
    return item, lease, raw, result


def dispatch(api, key):
    work = api.call("POST", f"/{key}/claim", {"owner": "dispatch-" + uuid.uuid4().hex})
    lease = {"workKey": key, "owner": work["owner"], "fence": work["fence"]}
    return api.call("POST", f"/{key}/dispatch-raw", lease)


def test_raw_intent_survives_api_absence_then_dispatches_once(db, tmp_path):
    item, lease, _raw, result = ready_intent(db, tmp_path)
    # Nothing called the notification API during collection.
    with db.cursor() as cur:
        cur.execute(
            "SELECT outbox_id FROM pipeline_recovery_raw_intent WHERE work_key=%s",
            (lease.key,),
        )
        assert cur.fetchone() == (None,)
    db.rollback()
    with closing(Controller(os.environ["RECOVERY_TEST_URL_B"], "test-only")) as api:
        assert dispatch(api, lease.key) == {"enqueued": True}
        assert (
            api.call("GET", f"/{lease.key}/receipts/outbox-{result['hash']}")["to"]
            == "ETL_READY"
        )
        with pytest.raises(CoordinationError):
            dispatch(api, lease.key)
    with db.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pipeline_outbox WHERE idempotency_key=%s",
            ("COORDINATED_RAW_READY:" + lease.key,),
        )
        assert cur.fetchone() == (1,)
        cur.execute(
            "SELECT checkpoint FROM pipeline_job_item WHERE id=%s", (item.item_id,)
        )
        assert cur.fetchone()[0]["rawReadyNotificationPending"] is False
    db.rollback()


@pytest.mark.parametrize("failure", ["cancel", "hash", "outbox_collision"])
def test_dispatch_refuses_conflict_without_advancing(db, tmp_path, failure):
    item, lease, _raw, result = ready_intent(db, tmp_path)
    with db.cursor() as cur:
        if failure == "cancel":
            cur.execute(
                "UPDATE pipeline_job SET status='CANCEL_REQUESTED' WHERE id=%s",
                (item.job_id,),
            )
        elif failure == "hash":
            cur.execute(
                "UPDATE source_object SET content_hash=%s WHERE id=%s",
                ("a" * 64, result["sourceIds"][0]),
            )
        else:
            cur.execute(
                "INSERT INTO pipeline_outbox(idempotency_key,payload) VALUES(%s,'{}')",
                ("COORDINATED_RAW_READY:" + lease.key,),
            )
    db.commit()
    with closing(Controller(os.environ["RECOVERY_TEST_URL_B"], "test-only")) as api:
        with pytest.raises(CoordinationError):
            dispatch(api, lease.key)
        assert api.call("GET", f"/{lease.key}")["phase"] == "RAW_READY"
    with db.cursor() as cur:
        cur.execute(
            "SELECT outbox_id FROM pipeline_recovery_raw_intent WHERE work_key=%s",
            (lease.key,),
        )
        assert cur.fetchone() == (None,)
    db.rollback()


def test_job_cancellation_stops_sends_and_commit(db, tmp_path):
    item = make_item(db)
    guard = JobGuard(db, item)
    lease = lease_for()
    lease.stopped = guard.stopped
    try:
        assert not guard.stopped()
        with db.cursor() as cur:
            cur.execute(
                "UPDATE pipeline_job SET status='CANCEL_REQUESTED' WHERE id=%s",
                (item.job_id,),
            )
        db.commit()
        with pytest.raises(CoordinationError, match="lease_lost"):
            lease.reserve("page")
        assert lease.api.call("GET", f"/{lease.key}")["state"] == "PAUSED"
        with pytest.raises(CoordinationError, match="lease_lost"):
            commit_raw(db, lease, [source(tmp_path)], tmp_path, item)
    finally:
        lease.close()
        guard.close()


@pytest.mark.parametrize("provider_name", ["KIS", "SEC"])
def test_real_collector_path_raw_commit(
    db, tmp_path, provider, monkeypatch, provider_name
):
    from datetime import date

    from collectors import kis_client, sec_client
    from workers import coordinated_collector as worker

    url, state = provider
    item = make_item(db)
    args = SimpleNamespace(
        output_dir=str(tmp_path),
        controller_url=os.environ["RECOVERY_TEST_URL"],
        admin_api_key="test-only",
    )
    monkeypatch.setenv("RAW_STORAGE_BASE_URI", "")
    monkeypatch.setenv("RAW_STORAGE_BACKEND", "LOCAL")
    monkeypatch.setenv(
        f"RECOVERY_{provider_name}_QUOTA_SCOPE", "test-" + str(item.item_id)
    )
    if provider_name == "KIS":
        target = {
            "asset_id": 1,
            "ticker": "TEST",
            "exchange": "NASDAQ",
            "provider_symbol": "TEST",
            "provider_exchange_code": "NAS",
        }
        options = {
            "start_date": date(2026, 1, 1),
            "end_date": date(2026, 1, 2),
            "adjusted": False,
            "max_pages": 1,
            "source": "KIS",
            "interval": "1D",
            "currency": "USD",
        }
        contract = {
            "target": "asset:1",
            "exchange": "NAS",
            "symbol": "TEST",
            "start": "2026-01-01",
            "end": "2026-01-02",
            "adjusted": False,
            "maxPages": 1,
            "interval": "1D",
            "currency": "USD",
            "source": "KIS",
            "requestIntervalMs": 1000,
        }
        monkeypatch.setattr(kis_client, "KIS_BASE_URL", url)
        for key in ("APPKEY", "APPSECRET", "ACCESS_TOKEN"):
            monkeypatch.setenv(key, "test-only")
        monkeypatch.delenv("ACCESS_TOKEN_TOKEN_EXPIRED", raising=False)
        state["responses"] = [
            (
                200,
                {
                    "rt_cd": "0",
                    "output2": [
                        {
                            "xymd": "20260101",
                            "open": "1",
                            "high": "2",
                            "low": "1",
                            "clos": "2",
                            "tvol": "10",
                        }
                    ],
                },
            )
        ]
    else:
        target = {"company_id": 1, "cik": "0000000001"}
        options = SimpleNamespace(include_history=False, all_facts=False)
        contract = {
            "target": "company:1",
            "cik": "0000000001",
            "includeHistory": False,
            "allFacts": False,
            "requestIntervalMs": 1000,
        }
        monkeypatch.setattr(sec_client, "SEC_DATA_URL", url)
        monkeypatch.setattr(
            "backfills.sec_financial.backfill.require_user_agent",
            lambda: "test test@example.invalid",
        )
        state["responses"] = [
            (200, {"cik": "0000000001", "filings": {"recent": {}}}),
            (200, {"cik": 1, "facts": {}}),
        ]
    generation = "test-generation"
    identity = {
        "provider": provider_name,
        "quotaScope": "test-" + str(item.item_id),
        "target": contract["target"],
        "dataset": "price-1D" if provider_name == "KIS" else "sec-all-history",
        "start": "2026-01-01",
        "end": "2026-01-02",
        "revision": canonical_hash({"generation": generation, "contract": contract}),
    }
    if provider_name == "SEC":
        identity.update(start="0001-01-01", end="9999-12-31")
    doc = {
        "version": "coordinated-v1",
        "generation": generation,
        "contract": contract,
        "identity": identity,
        "policy": policy(intervalMs=1000),
    }
    raw = json.dumps(doc).encode()
    path = tmp_path / "manifest.json"
    path.write_bytes(raw)
    args.recovery_manifest = str(path)
    args.recovery_manifest_sha256 = hashlib.sha256(raw).hexdigest()
    original_session = worker.DurableSession
    monkeypatch.setattr(
        worker,
        "DurableSession",
        lambda p, l, u: original_session(p, l, u, allowed_origins={url}),
    )
    # DB target resolution is stubbed; actual provider clients/raw files/Controller/DB commit run.
    monkeypatch.setattr(worker, "resolve", lambda *a: (target, options, contract))
    result = worker.process(db, item, args, provider_name)
    assert result["rawReadyNotificationPending"] is True
    with db.cursor() as cur:
        cur.execute(
            "SELECT status,checkpoint FROM pipeline_job_item WHERE id=%s",
            (item.item_id,),
        )
        status, checkpoint = cur.fetchone()
        assert status == "COLLECTED" and checkpoint["phase"] == "RAW_READY"
        cur.execute(
            "SELECT count(*) FROM source_object WHERE pipeline_job_item_id=%s",
            (item.item_id,),
        )
        assert cur.fetchone()[0] == (1 if provider_name == "KIS" else 2)
    db.rollback()
