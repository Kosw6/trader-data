"""Explicit bounded real-provider/Kafka crash proof. No legacy work adoption."""

import json
import os
import sys
import time
import traceback
from pathlib import Path
from types import SimpleNamespace

import psycopg2
from psycopg2.extras import Json

from pipeline.coordinated_news import plan_for
from pipeline.durable_requests import CoordinationError
from workers.coordinated_consumer import consume_once
from workers.coordinated_news_collector import collect_raw, controller
from workers.coordinated_news_consumer import (
    create_consumer,
    dispatch_pending,
    news_topic,
    trusted_plan,
)
from workers.coordinated_scheduler import audit
from workers.job_repository import claim_queued_item_by_id

ROOT = Path("/news-proof")
RAW = Path("/recovery-news")


def save(name, value):
    with (ROOT / name).open("x", encoding="utf8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())


def read(name):
    return json.loads((ROOT / name).read_text())


def db_connect():
    db = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=5)
    with db.cursor() as c:
        c.execute("SELECT current_schema()")
        assert c.fetchone() == ("control649",), "WRONG_SCHEMA"
    db.rollback()
    return db


def fingerprint(db, boundaries=None):
    result = {}
    tables = [
        "company",
        "asset",
        "filing",
        "financial_fact",
        "financial_metric",
        "financial_metric_mapping",
        "price_bar",
        "source_object",
        "record_lineage",
        "pipeline_job",
        "pipeline_job_item",
    ]
    with db.cursor() as c:
        for table in tables:
            if boundaries is None:
                c.execute("SELECT coalesce(max(id),0) FROM " + table)
                maximum = c.fetchone()[0]
            else:
                maximum = boundaries[table]["maxId"]
            c.execute(
                "SELECT count(*),md5(coalesce(string_agg(row_to_json(t)::text,E'\\n' ORDER BY id),'')) FROM "
                + table
                + " t WHERE id<=%s",
                (maximum,),
            )
            count, sha = c.fetchone()
            result[table] = {"maxId": maximum, "count": count, "hash": sha}
        keys = boundaries["news_collection_candidate"]["keys"] if boundaries else None
        if keys is None:
            c.execute(
                "SELECT url_hash FROM news_collection_candidate ORDER BY url_hash"
            )
            keys = [r[0] for r in c.fetchall()]
        c.execute(
            "SELECT count(*),md5(coalesce(string_agg(row_to_json(t)::text,E'\\n' ORDER BY url_hash),'')) FROM news_collection_candidate t WHERE url_hash=ANY(%s)",
            (keys,),
        )
        count, sha = c.fetchone()
        result["news_collection_candidate"] = {
            "keys": keys,
            "count": count,
            "hash": sha,
        }
        maximum = result["source_object"]["maxId"]
        c.execute(
            "SELECT count(*),md5(coalesce(string_agg(row_to_json(t)::text,E'\\n' ORDER BY url_hash,source_object_id,observation_hash),'')) FROM news_collection_observation t WHERE source_object_id<=%s",
            (maximum,),
        )
        count, sha = c.fetchone()
        result["news_collection_observation"] = {"count": count, "hash": sha}
    db.rollback()
    return result


def setup(provider):
    name = "setup-" + provider.lower() + ".json"
    assert not (ROOT / name).exists(), "SETUP_ALREADY_EXISTS"
    db = db_connect()
    api = controller()
    try:
        if not (ROOT / "baseline.json").exists():
            save("baseline.json", fingerprint(db))
        with db.cursor() as c:
            c.execute(
                "SELECT id FROM asset WHERE ticker='NVDA' AND asset_type='EQUITY'"
            )
            ids = c.fetchall()
            assert len(ids) == 1, "NVDA_NOT_UNIQUE"
        db.rollback()
        day = "2016-02-17" if provider == "RSS" else "2016-12-30"
        request = {
            "provider": provider,
            "assetIds": [ids[0][0]],
            "startDate": day,
            "endDate": day,
        }
        with api.session.post(
            "https://revestlab.com/api/controller/api/v1/admin/news/plan",
            json=request,
            timeout=15,
            allow_redirects=False,
        ) as r:
            assert r.status_code == 200, "NEWS_PLAN_NOT_READY"
            plan = r.json()
        assert plan["parameters"]["recoveryVersion"] == "news-coordinated-v1"
        unit = plan["itemKeys"][0] if provider == "RSS" else "GKG:20161230141500"
        assert unit in plan["itemKeys"], "UNIT_NOT_IN_APPROVED_PLAN"
        with db.cursor() as c:
            c.execute(
                "SELECT count(*) FROM pipeline_job_item WHERE status IN ('RUNNING','COLLECTING','PUBLISHING')"
            )
            assert c.fetchone() == (0,), "OTHER_ITEM_RUNNING"
            generation = "approved-news-recovery-" + provider.lower() + "-20261102"
            c.execute(
                "INSERT INTO pipeline_job(job_type,source,status,priority,requested_by,idempotency_key,parameters) VALUES(%s,'NEWS','QUEUED',0,'approved-news-recovery-verification',%s,%s) RETURNING id",
                (plan["jobType"], generation, Json(plan["parameters"])),
            )
            job = c.fetchone()[0]
            c.execute(
                "INSERT INTO pipeline_job_item(job_id,item_key,status) VALUES(%s,%s,'QUEUED') RETURNING id",
                (job, unit),
            )
            item_id = c.fetchone()[0]
        item = claim_queued_item_by_id(
            db, item_id, source="NEWS", job_type=plan["jobType"]
        )
        assert item and item.job_id == job
        save(
            name,
            {
                "item": vars(item),
                "boundedUnits": 1,
                "provider": provider,
                "planUsesRecovery": True,
            },
        )
        print(
            json.dumps(
                {
                    "status": "APPROVED_ONE_UNIT_CLAIMED",
                    "provider": provider,
                    "jobId": job,
                    "itemId": item_id,
                    "unit": unit,
                }
            )
        )
    finally:
        api.close()
        db.close()


def collect(provider, crash=False):
    db = db_connect()
    try:
        item = SimpleNamespace(**read("setup-" + provider.lower() + ".json")["item"])
        result = collect_raw(db, item, RAW)
        api = controller()
        try:
            work = api.call("GET", "/" + result["workKey"])
        finally:
            api.close()
        assert work["phase"] == "RAW_READY" and work["state"] == "READY", (
            "RAW_NOT_COMMITTED"
        )
        report = {
            "status": "RAW_COMMITTED_BEFORE_PROCESS_KILL" if crash else "RAW_COMMITTED",
            "provider": provider,
            "receipt": result,
            "requests": work["requests"],
            "deadline": work["deadline"],
        }
        save("raw-" + provider.lower() + ".json", report)
        print(json.dumps(report), flush=True)
        if crash:
            save(
                "kill-ready-raw.json",
                {
                    "point": "RAW_READY_BEFORE_KAFKA_DISPATCH",
                    "provider": provider,
                    "workKey": result["workKey"],
                },
            )
            while True:
                time.sleep(1)
    finally:
        db.close()


def dispatch(provider):
    db = db_connect()
    api = controller()
    key = read("raw-" + provider.lower() + ".json")["receipt"]["workKey"]
    try:
        work = api.call("GET", "/" + key)
        assert work["phase"] == "RAW_READY", "NOT_RAW_READY"
        keys = dispatch_pending(db, api, RAW)
        assert key in keys, "RAW_NOT_DISPATCHED"
        with db.cursor() as c:
            c.execute(
                "SELECT o.id,o.status,o.topic,o.kafka_partition,o.kafka_offset FROM pipeline_outbox o JOIN pipeline_recovery_raw_intent i ON i.outbox_id=o.id WHERE i.work_key=%s",
                (key,),
            )
            row = c.fetchone()
        db.rollback()
        save(
            "dispatch-" + provider.lower() + ".json",
            {"workKey": key, "outboxId": row[0], "topic": row[2]},
        )
        print(
            json.dumps(
                {
                    "status": "DURABLE_OUTBOX_DISPATCHED",
                    "provider": provider,
                    "workKey": key,
                    "outboxId": row[0],
                }
            )
        )
    finally:
        api.close()
        db.close()


def snapshot(db, key):
    with db.cursor() as c:
        c.execute(
            "SELECT phase,state,requests,retries FROM pipeline_recovery_work WHERE work_key=%s",
            (key,),
        )
        work = c.fetchone()
        c.execute(
            "SELECT sources FROM pipeline_recovery_raw_intent WHERE work_key=%s", (key,)
        )
        sid = c.fetchone()[0][0]["id"]
        c.execute(
            "SELECT count(*) FROM news_collection_observation WHERE source_object_id=%s",
            (sid,),
        )
        observations = c.fetchone()[0]
        c.execute(
            "SELECT count(*) FROM pipeline_recovery_receipt WHERE work_key=%s", (key,)
        )
        receipts = c.fetchone()[0]
    db.rollback()
    return {
        "phase": work[0],
        "state": work[1],
        "requests": work[2],
        "retries": work[3],
        "sourceId": sid,
        "observations": observations,
        "receipts": receipts,
    }


def consume(provider, crash=False, replay=False):
    db = db_connect()
    api = controller()
    consumer = create_consumer()
    key = read("raw-" + provider.lower() + ".json")["receipt"]["workKey"]
    topic = news_topic()
    from kafka import TopicPartition

    tp = TopicPartition(topic, 0)
    prior_offset = consumer.committed(tp)
    before = snapshot(db, key)

    class BeforeOffset:
        def poll(self, **kw):
            return consumer.poll(**kw)

        def commit(self, offsets):
            after = snapshot(db, key)
            assert (
                after["phase"] == after["state"] == "COMPLETE"
                and after["receipts"] == 6
            ), "DB_NOT_COMPLETE"
            assert (
                after["requests"]
                == read("raw-" + provider.lower() + ".json")["requests"]
            ), "PROVIDER_RECOLLECTED"
            assert fingerprint(db, read("baseline.json")) == read("baseline.json"), (
                "OLD_DATA_CHANGED"
            )
            expected = offsets[tp].offset
            assert consumer.committed(tp) == prior_offset, "OFFSET_COMMITTED_TOO_EARLY"
            save(
                "before-offset-" + provider.lower() + ".json",
                {
                    "workKey": key,
                    "beforeOffset": prior_offset,
                    "expectedNextOffset": expected,
                    "database": after,
                },
            )
            save(
                "kill-ready-offset.json",
                {
                    "point": "DB_COMPLETE_BEFORE_KAFKA_OFFSET_COMMIT",
                    "provider": provider,
                    "workKey": key,
                },
            )
            print(
                json.dumps(
                    {
                        "status": "DB_COMPLETE_OFFSET_UNCOMMITTED",
                        "provider": provider,
                        "offsetBefore": prior_offset,
                        "expectedNextOffset": expected,
                    }
                ),
                flush=True,
            )
            while True:
                time.sleep(1)

    try:
        used = BeforeOffset() if crash else consumer
        handled = False
        for _ in range(90):
            if consume_once(
                used,
                db,
                api,
                lambda k: trusted_plan(db, k) if k == key else {},
                RAW,
                topic=topic,
            ):
                handled = True
                break
        assert handled, "EXPECTED_KAFKA_EVENT_NOT_RECEIVED"
        after = snapshot(db, key)
        committed = consumer.committed(tp)
        assert (
            after["phase"] == after["state"] == "COMPLETE" and after["receipts"] == 6
        ), "FINAL_NOT_COMPLETE"
        if replay:
            saved = read("before-offset-" + provider.lower() + ".json")
            assert (
                after == saved["database"] and committed == saved["expectedNextOffset"]
            ), "DUPLICATE_REPLAY_CHANGED_DB_OR_OFFSET"
        assert (
            after["requests"] == read("raw-" + provider.lower() + ".json")["requests"]
        ), "PROVIDER_REQUEST_COUNT_CHANGED"
        assert fingerprint(db, read("baseline.json")) == read("baseline.json"), (
            "OLD_DATA_CHANGED"
        )
        checked = audit(db, {key: plan_for(key)})
        assert checked["unresolvedTotal"] == 0, "UNRESOLVED_COMMIT"
        with db.cursor() as c:
            c.execute(
                "SELECT o.status,o.kafka_partition,o.kafka_offset,o.topic FROM pipeline_outbox o JOIN pipeline_recovery_raw_intent i ON i.outbox_id=o.id WHERE i.work_key=%s",
                (key,),
            )
            outbox = c.fetchone()
        db.rollback()
        assert (
            outbox[0] == "PUBLISHED"
            and outbox[1] == 0
            and outbox[2] + 1 == committed
            and outbox[3] == topic
        ), "KAFKA_ACK_NOT_MATCHED"
        result = {
            "status": "REAL_KAFKA_REPLAY_VERIFIED"
            if replay
            else "REAL_KAFKA_TRANSFER_VERIFIED",
            "provider": provider,
            "workKey": key,
            "database": after,
            "audit": checked,
            "kafka": {
                "outboxStatus": outbox[0],
                "partition": outbox[1],
                "offset": outbox[2],
                "group": os.environ["NEWS_RECOVERY_GROUP"],
                "committedNextOffset": committed,
            },
            "extraProviderRequests": 0,
            "existingDataUnchanged": True,
            "duplicateDatabaseWrites": 0 if replay else None,
        }
        save("verified-" + provider.lower() + ".json", result)
        print(json.dumps(result))
    finally:
        consumer.close()
        api.close()
        db.close()


if __name__ == "__main__":
    assert os.environ.get("APPROVED_NEWS_CRASH_PROOF") == "news-recovery-20261102", (
        "EXPLICIT_OPT_IN_REQUIRED"
    )
    action, provider = sys.argv[1:]
    assert provider in {"RSS", "GDELT"}
    try:
        if action == "setup":
            setup(provider)
        elif action == "collect-crash":
            collect(provider, True)
        elif action == "collect":
            collect(provider)
        elif action == "dispatch":
            dispatch(provider)
        elif action == "consume-crash":
            consume(provider, True)
        elif action == "replay":
            consume(provider, replay=True)
        elif action == "consume":
            consume(provider)
        else:
            raise AssertionError("INVALID_PROOF_ACTION")
    except Exception as e:
        print(
            json.dumps(
                {
                    "status": "HELD_DO_NOT_REPLAY",
                    "errorType": type(e).__name__,
                    "safeCode": e.code
                    if isinstance(e, CoordinationError)
                    else str(e)
                    if isinstance(e, AssertionError)
                    else None,
                    "sqlState": getattr(e, "pgcode", None),
                    "frames": [
                        {
                            "file": f.filename.rsplit("/", 1)[-1],
                            "line": f.lineno,
                            "function": f.name,
                        }
                        for f in traceback.extract_tb(e.__traceback__)
                    ],
                }
            ),
            flush=True,
        )
        raise SystemExit(1) from None
