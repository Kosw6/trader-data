"""Dedicated NEWS recovery topic; Kafka offsets follow independently verified DB commits."""

import json
import os
import re
import signal
import time
from contextlib import closing

import psycopg2

from pipeline.coordinated_news import VERSION, plan_for
from pipeline.durable_requests import CoordinationError
from workers.coordinated_consumer import consume_once
from workers.coordinated_news_collector import controller
from workers.coordinated_recovery import advance


def news_topic():
    value = os.getenv("NEWS_RECOVERY_TOPIC", "trader.raw.coordinated.news.ready")
    if value not in {
        "trader.raw.coordinated.news.ready",
        "trader.raw.coordinated.news.ready649",
    }:
        raise CoordinationError("invalid_contract")
    return value


def trusted_plan(db, key):
    if not isinstance(key, str) or not re.fullmatch("[0-9a-f]{64}", key):
        raise CoordinationError("invalid_contract")
    with db.cursor() as cur:
        cur.execute(
            "SELECT w.identity,i.contract FROM pipeline_recovery_work w JOIN pipeline_recovery_raw_intent i USING(work_key) WHERE w.work_key=%s",
            (key,),
        )
        row = cur.fetchone()
    db.rollback()
    if (
        not row
        or row[0]["provider"] != "NEWS"
        or row[1].get("version") != VERSION
        or row[0]["target"] != row[1].get("target")
    ):
        raise CoordinationError("contract_conflict")
    return {key: plan_for(key)}


def dispatch_pending(db, api, root):
    # Recover only a committed raw checkpoint. Never fetch/replay a provider or a legacy job.
    with db.cursor() as cur:
        cur.execute(
            """SELECT w.work_key FROM pipeline_recovery_work w JOIN pipeline_recovery_raw_intent i USING(work_key)
          WHERE w.identity->>'provider'='NEWS' AND i.contract->>'version'=%s AND w.phase='RAW_READY'
          AND w.state IN ('READY','LEASED') AND (w.lease_until IS NULL OR w.lease_until<clock_timestamp())
          AND w.deadline>clock_timestamp() AND NOT EXISTS(SELECT 1 FROM pipeline_recovery_request r WHERE r.work_key=w.work_key AND r.outcome='PENDING')
          ORDER BY w.work_key LIMIT 10""",
            (VERSION,),
        )
        keys = [r[0] for r in cur.fetchall()]
    db.rollback()
    for key in keys:
        advance(db, api, trusted_plan(db, key)[key], root, enabled=True, max_steps=1)
    return keys


def create_consumer():
    from kafka import KafkaConsumer

    group = os.environ["NEWS_RECOVERY_GROUP"]
    if not group.startswith("coordinated-news-"):
        raise CoordinationError("invalid_contract")
    return KafkaConsumer(
        news_topic(),
        bootstrap_servers=os.environ["KAFKA_BROKERS"].split(","),
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=1,
        max_poll_interval_ms=900000,
        request_timeout_ms=30000,
        allow_auto_create_topics=False,
    )


def main():
    if os.getenv("NEWS_RECOVERY_ENABLED") != "true":
        raise CoordinationError("invalid_contract")
    stopped = []
    signal.signal(signal.SIGTERM, lambda *_: stopped.append(True))
    signal.signal(signal.SIGINT, lambda *_: stopped.append(True))
    root = os.environ["NEWS_RECOVERY_RAW_ROOT"]
    with (
        closing(psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=5)) as db,
        closing(controller()) as api,
    ):
        consumer = create_consumer()
        try:
            while not stopped:
                try:
                    dispatch_pending(db, api, root)
                    consume_once(
                        consumer,
                        db,
                        api,
                        lambda k: trusted_plan(db, k),
                        root,
                        topic=news_topic(),
                    )
                except CoordinationError as error:
                    if error.code != "lease_lost":
                        raise
                    # A definitive live-owner rejection is not an uncertain
                    # commit. Rejoin at the last committed offset; never skip
                    # the polled message while waiting for the old lease.
                    db.rollback()
                    consumer.close()
                    for _ in range(2):
                        if stopped:
                            break
                        time.sleep(1)
                    if not stopped:
                        consumer = create_consumer()
        finally:
            consumer.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:  # noqa: BLE001 -- no credentials, provider text or SQL in process output
        print(
            json.dumps(
                {
                    "state": "HELD",
                    "offsetState": "UNCONFIRMED",
                    "automaticProviderRetry": False,
                }
            )
        )
        raise SystemExit(1) from None
