"""Opt-in coordinated topic consumer. DB receipts precede explicit offsets.

Events select only an already approved local plan; never paths supplied by Kafka.
On failure, do not acknowledge or fetch later records. Restart reconciles DB state.
"""

import argparse
import json
import os
from contextlib import closing
from pathlib import Path

import psycopg2

from pipeline.durable_requests import Controller, CoordinationError
from workers.coordinated_recovery import advance, pinned_plan

TOPIC = "trader.raw.coordinated.ready"


def recovery_topic():
    topic = os.getenv("RECOVERY_RAW_TOPIC", TOPIC)
    if topic not in {"trader.raw.coordinated.ready", "trader.raw.coordinated.ready649"}:
        raise CoordinationError("invalid_contract")
    return topic


def load_registry(path, digest):
    import hashlib

    path = Path(path).resolve()
    raw = path.read_bytes()
    if len(raw) > 1_000_000 or hashlib.sha256(raw).hexdigest() != digest:
        raise CoordinationError("invalid_contract")
    entries = json.loads(raw)
    if not isinstance(entries, list) or not 1 <= len(entries) <= 1000:
        raise CoordinationError("invalid_contract")
    registry = {}
    for entry in entries:
        plan_path = (path.parent / entry["path"]).resolve()
        if not plan_path.is_relative_to(path.parent):
            raise CoordinationError("invalid_contract")
        plan = pinned_plan(plan_path, entry["sha256"])
        if plan["workKey"] in registry:
            raise CoordinationError("contract_conflict")
        registry[plan["workKey"]] = plan
    return registry


def process_event(db, api, payload, registry, raw_root, *, topic=None):
    if (
        not isinstance(payload, dict)
        or payload.get("eventVersion") != 2
        or payload.get("eventType") != "COORDINATED_RAW_READY"
    ):
        raise CoordinationError("invalid_contract")
    key = payload.get("workKey")
    if key not in registry:
        raise CoordinationError("review_required")
    with db.cursor() as cur:
        cur.execute(
            """SELECT o.payload FROM pipeline_recovery_raw_intent i
          JOIN pipeline_outbox o ON o.id=i.outbox_id WHERE i.work_key=%s AND
          o.idempotency_key=%s AND o.topic=%s AND o.message_key=%s""",
            (key, "COORDINATED_RAW_READY:" + key, topic or recovery_topic(), key),
        )
        row = cur.fetchone()
    db.rollback()
    if not row or row[0] != payload:
        raise CoordinationError("contract_conflict")
    advance(db, api, registry[key], raw_root, enabled=True, max_steps=5)
    # A duplicate only becomes acknowledged after validating committed evidence.
    with db.cursor() as cur:
        cur.execute(
            """SELECT w.phase,w.state,c.content_hash,r.content_hash FROM pipeline_recovery_work w
          JOIN pipeline_recovery_candidate c USING(work_key)
          JOIN pipeline_recovery_receipt r ON r.work_key=w.work_key AND r.from_phase='PUBLISHED' AND r.to_phase='COMPLETE'
          WHERE w.work_key=%s""",
            (key,),
        )
        committed = cur.fetchone()
    db.rollback()
    if (
        not committed
        or committed[:2] != ("COMPLETE", "COMPLETE")
        or committed[2] != committed[3]
    ):
        raise CoordinationError("review_required")
    # Deferred import: scheduler shares the registry parser but is never launched here.
    from workers.coordinated_scheduler import audit

    if audit(db, {key: registry[key]})["unresolvedTotal"]:
        raise CoordinationError("contract_conflict")
    if registry[key].get("kind") == "NEWS":
        # Also re-read actual rows on duplicate delivery, not only phase receipts.
        # No Kafka offset may acknowledge a row/source changed after completion.
        from pipeline.coordinated_news import news_publication_verifier
        from pipeline.db_connections import independent_connection
        from pipeline.durable_requests import canonical_hash

        with closing(independent_connection(db)) as read:
            read.set_session(readonly=True, isolation_level="REPEATABLE READ")
            with read.cursor() as cur:
                news_publication_verifier(raw_root)(
                    cur, key, canonical_hash(registry[key])
                )
            read.rollback()
    return "VERIFIED"


def consume_once(consumer, db, api, registry, raw_root, *, topic=None):
    from kafka import TopicPartition
    from kafka.structs import OffsetAndMetadata

    records = consumer.poll(timeout_ms=1000, max_records=1)
    for messages in records.values():
        for message in messages:
            if (
                message.topic != (topic or recovery_topic())
                or len(message.value) > 1_000_000
            ):
                raise CoordinationError("invalid_contract")
            payload = json.loads(message.value)
            if message.key != payload.get("workKey", "").encode():
                raise CoordinationError("contract_conflict")
            approved = (
                registry(payload.get("workKey")) if callable(registry) else registry
            )
            process_event(db, api, payload, approved, raw_root, topic=topic)
            offset = {"offset": message.offset + 1, "metadata": ""}
            if "leader_epoch" in OffsetAndMetadata._fields:
                offset["leader_epoch"] = -1
            consumer.commit(
                {
                    TopicPartition(message.topic, message.partition): OffsetAndMetadata(
                        **offset
                    )
                }
            )
            return True
    return False


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--registry", required=True)
    p.add_argument("--registry-sha256", required=True)
    p.add_argument("--raw-root", required=True)
    p.add_argument("--max-polls", type=int, default=1)
    args = p.parse_args()
    if (
        os.getenv("COORDINATED_STAGES_ENABLED") != "1"
        or not 1 <= args.max_polls <= 1000
    ):
        raise CoordinationError("invalid_contract")
    from kafka import KafkaConsumer

    registry = load_registry(args.registry, args.registry_sha256)
    # A separate consumer group is mandatory. Legacy group defaults are not used.
    group = os.environ["COORDINATED_KAFKA_GROUP"]
    if not group.startswith("coordinated-"):
        raise CoordinationError("invalid_contract")
    with (
        closing(
            Controller(os.environ["CONTROLLER_URL"], os.environ["ADMIN_API_KEY"])
        ) as api,
        closing(psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=5)) as db,
        closing(
            KafkaConsumer(
                recovery_topic(),
                bootstrap_servers=os.environ["COORDINATED_KAFKA_BROKERS"].split(","),
                group_id=group,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                max_poll_records=1,
                max_poll_interval_ms=900000,
                request_timeout_ms=30000,
            )
        ) as consumer,
    ):
        for _ in range(args.max_polls):
            consume_once(consumer, db, api, registry, args.raw_root)


if __name__ == "__main__":
    try:
        main()
    except Exception:  # noqa: BLE001 -- redact connection secrets at CLI boundary
        # No Kafka/DB connection strings, headers or provider body in logs.
        print(
            '{"state":"HELD","offsetState":"UNCONFIRMED","action":"inspect durable receipts"}'
        )
        raise SystemExit(1) from None
