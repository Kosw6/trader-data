import argparse
import json
import logging
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from psycopg2.extras import Json

from backfills.sec_financial.backfill import apply_company_financials
from transformers.sec_transformer import transform_filings, transform_financial_facts
from workers.db import connect_database
from workers.event_repository import find_processed_event, record_processed_event
from workers.heartbeat import WorkerHeartbeatReporter
from workers.raw_repository import (
    load_source_object,
    load_source_objects_for_item,
    mark_source_objects_status,
    read_raw_payload,
    source_object_completed_with_lineage,
)
from workers.sec_financial_worker import (
    latest_accession,
    load_single_target,
    update_collection_state,
)


LOGGER = logging.getLogger("sec-financial-etl-worker")


def parse_args():
    """SEC raw ETL-only worker 실행 옵션을 읽는다."""
    parser = argparse.ArgumentParser(
        description=(
            "SEC RAW_OBJECT_READY 이벤트 또는 source_object_id를 받아 "
            "같은 job item의 SEC raw 묶음을 filing/financial_fact로 적재합니다."
        )
    )
    parser.add_argument("--source-object-id", type=int)
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_RAW_SEC_TOPIC", "trader.raw.sec.ready"))
    parser.add_argument("--group-id", default=os.getenv("KAFKA_RAW_SEC_GROUP_ID", "trader-data-sec-etl"))
    parser.add_argument(
        "--consumer-name",
        default=os.getenv("KAFKA_RAW_SEC_CONSUMER_NAME", "sec-financial-etl-worker"),
    )
    parser.add_argument("--all-facts", action="store_true")
    parser.add_argument("--check-interval-hours", type=int, default=168)
    parser.add_argument("--max-records", type=int, default=0)
    parser.add_argument("--output-dir", default="pipeline_output/sec_financial_etl_worker")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def create_consumer(args):
    """SEC RAW_OBJECT_READY 전용 Kafka consumer를 생성한다."""
    try:
        from kafka import KafkaConsumer
    except ModuleNotFoundError as exc:
        raise RuntimeError("kafka-python is required. Install trader-data requirements first.") from exc

    return KafkaConsumer(
        args.topic,
        bootstrap_servers=[broker.strip() for broker in args.brokers.split(",") if broker.strip()],
        group_id=args.group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def read_bool(parameters, key, default):
    """job parameters의 boolean 값을 안전하게 읽는다."""
    value = parameters.get(key)
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def source_preview(source_object):
    """DB에서 읽은 source_object를 backfill apply 함수가 기대하는 dict로 바꾼다."""
    return {
        "provider": source_object["provider"],
        "data_type": source_object["data_type"],
        "external_id": source_object["external_id"],
        "pipeline_job_item_id": source_object["pipeline_job_item_id"],
        "storage_backend": source_object["storage_backend"],
        "storage_key": source_object["storage_key"],
        "object_uri": source_object["object_uri"],
        "object_size": source_object["object_size"],
        "content_hash": source_object["content_hash"],
        "dedupe_key": source_object["dedupe_key"],
        "collected_at": source_object["collected_at"],
        "processing_status": source_object["processing_status"],
    }


def load_sec_source_group(connection, trigger_source_object):
    """대표 source_object가 속한 job item의 SEC raw 객체 묶음을 조회한다."""
    item_id = trigger_source_object.get("pipeline_job_item_id")
    if not item_id:
        return [trigger_source_object]
    return load_source_objects_for_item(connection, item_id, provider="SEC")


def records_from_sec_sources(source_objects, args):
    """SEC raw 객체 묶음을 filing/financial_fact records로 변환한다."""
    payload_by_id = {source["id"]: read_raw_payload(source) for source in source_objects}
    source_by_type = {}
    for source in source_objects:
        source_by_type.setdefault(source["data_type"], []).append(source)

    submissions_sources = source_by_type.get("SUBMISSIONS", [])
    if not submissions_sources:
        raise ValueError("SEC ETL requires at least one SUBMISSIONS source_object")

    base_submissions_source = submissions_sources[0]
    base_submissions = payload_by_id[base_submissions_source["id"]]
    processed_at = datetime.now(timezone.utc)
    filing_records = []
    for source in submissions_sources:
        submissions = payload_by_id[source["id"]]
        filing_records.extend(
            transform_filings(
                submissions,
                [submissions.get("filings", {}).get("recent", {})],
                source["object_uri"],
                processed_at,
            )
        )

    for source in source_by_type.get("SUBMISSIONS_HISTORY", []):
        page = payload_by_id[source["id"]]
        filing_records.extend(
            transform_filings(
                base_submissions,
                [page],
                source["object_uri"],
                processed_at,
            )
        )

    all_facts = read_bool(base_submissions_source.get("parameters") or {}, "allFacts", args.all_facts)
    financial_fact_records = []
    for source in source_by_type.get("COMPANY_FACTS", []):
        company_facts = payload_by_id[source["id"]]
        financial_fact_records.extend(
            transform_financial_facts(
                company_facts,
                source["object_uri"],
                processed_at,
                core_metrics_only=not all_facts,
            )
        )

    filing_records = list(
        {record["accession_number"]: record for record in filing_records}.values()
    )
    return {
        "source_object": [source_preview(source) for source in source_objects],
        "filing": filing_records,
        "financial_fact": financial_fact_records,
        "company_facts_missing": len(source_by_type.get("COMPANY_FACTS", [])) == 0,
    }


def write_etl_previews(args, source_object_id, records):
    """SEC ETL 변환 결과를 JSONL preview로 남긴다."""
    output_root = Path(args.output_dir) / f"source-object-{source_object_id}" / "tables"
    output_root.mkdir(parents=True, exist_ok=True)
    for table_name in ("source_object", "filing", "financial_fact"):
        rows = records.get(table_name, [])
        path = output_root / f"{table_name}.jsonl"
        with path.open("w", encoding="utf-8", newline="\n") as output:
            for row in rows:
                output.write(json.dumps(row, ensure_ascii=False, default=str))
                output.write("\n")
        LOGGER.info("%s rows=%d path=%s", table_name, len(rows), path)


def update_item_etl_checkpoint(connection, source_object, checkpoint):
    """수집 item checkpoint에 ETL 결과 요약을 덧붙인다."""
    item_id = source_object.get("pipeline_job_item_id")
    if not item_id:
        return
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE pipeline_job_item
            SET checkpoint = COALESCE(checkpoint, '{}'::jsonb)
                || jsonb_build_object('etl', %s::jsonb)
            WHERE id = %s
            """,
            (Json(checkpoint), item_id),
        )


def build_checkpoint(trigger_source_object, target, stats, source_objects):
    """SEC ETL 결과 요약을 만든다."""
    return {
        "worker": "sec_financial_etl_worker",
        "mode": "ETL_ONLY",
        "source_object_id": trigger_source_object["id"],
        "pipeline_job_item_id": trigger_source_object.get("pipeline_job_item_id"),
        "source_object_ids": [source["id"] for source in source_objects],
        "ticker": target.get("ticker"),
        "cik": target.get("cik"),
        "company_id": target.get("company_id"),
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "stats": dict(sorted(stats.items())),
    }


def process_source_object(connection, source_object_id, args):
    """SEC 대표 source_object를 기준으로 같은 item의 raw 묶음을 정규화 테이블에 적재한다."""
    trigger_source_object = load_source_object(connection, source_object_id)
    if trigger_source_object["provider"] != "SEC":
        raise ValueError(f"unsupported provider for SEC ETL: {trigger_source_object['provider']}")

    target, target_ref = load_single_target(connection, trigger_source_object["item_key"])
    if target is None:
        raise LookupError(f"SEC target not found for item_key={trigger_source_object['item_key']}: {target_ref}")

    source_objects = load_sec_source_group(connection, trigger_source_object)
    source_object_ids = [source["id"] for source in source_objects]
    mark_source_objects_status(connection, source_object_ids, "PROCESSING")
    connection.commit()

    stats = Counter()
    skipped_facts = []
    try:
        records = records_from_sec_sources(source_objects, args)
        write_etl_previews(args, source_object_id, records)
        stats["source_objects"] = len(records["source_object"])
        stats["filings_seen"] = len(records["filing"])
        stats["facts_seen"] = len(records["financial_fact"])
        collection_status = "SUCCESS"
        error_code = None
        error_message = None
        if records.get("company_facts_missing"):
            stats["skipped_no_company_facts"] += 1
            collection_status = "SKIPPED"
            error_code = "SEC_NO_COMPANY_FACTS"
            error_message = "SEC companyfacts raw가 없어 financial_fact 적재를 건너뜁니다."
        elif not records["financial_fact"]:
            stats["skipped_no_core_facts"] += 1
            collection_status = "SKIPPED"
            error_code = "SEC_NO_CORE_FACTS"
            error_message = "SEC companyfacts raw에는 있으나 현재 기준 core financial fact가 없습니다."
        else:
            apply_company_financials(
                connection,
                target,
                records,
                datetime.now(timezone.utc),
                stats,
                skipped_facts,
            )

        stats["skipped_facts"] = len(skipped_facts)
        latest_seen_accession = latest_accession(records["filing"])
        checkpoint = build_checkpoint(trigger_source_object, target, stats, source_objects)
        checkpoint["latest_accession_number"] = latest_seen_accession
        update_item_etl_checkpoint(connection, trigger_source_object, checkpoint)
        update_collection_state(
            connection,
            target["company_id"],
            datetime.now(timezone.utc),
            args.check_interval_hours,
            collection_status,
            last_seen_external_id=latest_seen_accession,
            error_code=error_code,
            error_message=error_message,
            checkpoint=checkpoint,
        )
        mark_source_objects_status(connection, source_object_ids, "PROCESSED")
        connection.commit()
        return checkpoint
    except Exception as error:
        connection.rollback()
        mark_source_objects_status(connection, source_object_ids, "FAILED", error_message=str(error))
        connection.commit()
        raise


def process_kafka_message(connection, message, args):
    """RAW_OBJECT_READY Kafka message 하나를 멱등 처리한다."""
    payload = message.value
    idempotency_key = payload.get("idempotencyKey")
    source_object_id = int(payload.get("sourceObjectId") or 0)
    if not idempotency_key or source_object_id <= 0:
        raise ValueError(f"invalid RAW_OBJECT_READY payload: {payload}")

    already_processed = find_processed_event(connection, args.consumer_name, idempotency_key)
    if already_processed:
        if already_processed["status"] == "FAILED":
            LOGGER.info(
                "retrying previously failed SEC raw event idempotency_key=%s",
                idempotency_key,
            )
        else:
            LOGGER.info(
                "duplicate SEC raw event skipped idempotency_key=%s previous_status=%s",
                idempotency_key,
                already_processed["status"],
            )
            return "duplicate"

    if source_object_completed_with_lineage(connection, source_object_id):
        LOGGER.info(
            "SEC source_object already processed; recording missing consumer success source_object_id=%s",
            source_object_id,
        )
        record_processed_event(
            connection,
            args.consumer_name,
            idempotency_key,
            message.topic,
            message.partition,
            message.offset,
            "SUCCESS",
        )
        connection.commit()
        return "already_processed_source_object"

    try:
        process_source_object(connection, source_object_id, args)
        record_processed_event(
            connection,
            args.consumer_name,
            idempotency_key,
            message.topic,
            message.partition,
            message.offset,
            "SUCCESS",
        )
        connection.commit()
        return "success"
    except Exception as error:
        LOGGER.exception("SEC raw ETL failed source_object_id=%s", source_object_id)
        record_processed_event(
            connection,
            args.consumer_name,
            idempotency_key,
            message.topic,
            message.partition,
            message.offset,
            "FAILED",
            error_message=str(error),
        )
        connection.commit()
        return "failed"


def main():
    """SEC ETL-only worker entrypoint."""
    load_dotenv()
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    connection = connect_database()
    heartbeat = WorkerHeartbeatReporter.from_env(
        args.consumer_name,
        metadata={
            "module": "workers.sec_financial_etl_worker",
            "topic": args.topic,
            "groupId": args.group_id,
            "brokers": args.brokers,
        },
    )
    heartbeat.start()
    failed = False
    try:
        if args.source_object_id:
            heartbeat.mark_busy(processing_source_objects=1, metadata={"sourceObjectId": args.source_object_id})
            checkpoint = process_source_object(connection, args.source_object_id, args)
            heartbeat.mark_idle(metadata={"lastResult": "source-object", "sourceObjectId": args.source_object_id})
            print(json.dumps(checkpoint, ensure_ascii=False, indent=2, default=str))
            return

        consumer = create_consumer(args)
        processed_count = 0
        try:
            for message in consumer:
                heartbeat.mark_busy(
                    processing_source_objects=1,
                    metadata={
                        "lastTopic": message.topic,
                        "lastPartition": message.partition,
                        "lastOffset": message.offset,
                    },
                )
                result = process_kafka_message(connection, message, args)
                consumer.commit()
                processed_count += 1
                heartbeat.mark_idle(
                    metadata={
                        "lastResult": result,
                        "lastCommittedTopic": message.topic,
                        "lastCommittedPartition": message.partition,
                        "lastCommittedOffset": message.offset,
                    },
                )
                LOGGER.info(
                    "SEC raw message committed topic=%s partition=%s offset=%s result=%s",
                    message.topic,
                    message.partition,
                    message.offset,
                    result,
                )
                if args.max_records and processed_count >= args.max_records:
                    break
        finally:
            consumer.close()
    except Exception as exc:
        failed = True
        heartbeat.mark_error(metadata={"lastError": str(exc)})
        raise
    finally:
        heartbeat.stop(status="ERROR" if failed else "STOPPED")
        connection.close()


if __name__ == "__main__":
    main()
