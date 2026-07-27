import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from psycopg2.extras import Json

from backfills.bls_macro.backfill import apply_bls_macro_records
from collectors.bls_collector import build_bls_lineage
from transformers.bls_transformer import (
    transform_macro_series,
    transform_observation_statuses,
    transform_observation_vintages,
    transform_observations,
)
from workers.db import connect_database
from workers.event_repository import find_processed_event, record_processed_event
from workers.heartbeat import WorkerHeartbeatReporter
from workers.raw_repository import (
    load_source_object,
    mark_source_object_status,
    read_raw_payload,
    source_object_completed_with_lineage,
)


LOGGER = logging.getLogger("bls-macro-etl-worker")


def parse_args():
    """BLS raw ETL-only worker 실행 옵션을 읽는다."""
    parser = argparse.ArgumentParser(
        description=(
            "BLS RAW_OBJECT_READY 이벤트 또는 source_object_id를 받아 "
            "macro_* 테이블과 record_lineage를 적재합니다."
        )
    )
    parser.add_argument("--source-object-id", type=int)
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_RAW_BLS_TOPIC", "trader.raw.bls.ready"))
    parser.add_argument("--group-id", default=os.getenv("KAFKA_RAW_BLS_GROUP_ID", "trader-data-bls-etl"))
    parser.add_argument(
        "--consumer-name",
        default=os.getenv("KAFKA_RAW_BLS_CONSUMER_NAME", "bls-macro-etl-worker"),
    )
    parser.add_argument("--max-records", type=int, default=0)
    parser.add_argument("--output-dir", default="pipeline_output/bls_macro_etl_worker")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def create_consumer(args):
    """BLS RAW_OBJECT_READY 전용 Kafka consumer를 생성한다."""
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


def load_series_metadata(connection, series_ids):
    """raw 응답에 포함된 BLS series id의 메타데이터를 DB에서 조회한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT external_series_id, name, category, frequency, unit, seasonal_adjustment
            FROM macro_series
            WHERE provider = 'BLS'
              AND external_series_id = ANY(%s)
            """,
            (list(series_ids),),
        )
        rows = cursor.fetchall()

    metadata = {
        row[0]: {
            "name": row[1],
            "category": row[2],
            "frequency": row[3],
            "unit": row[4],
            "seasonal_adjustment": row[5],
        }
        for row in rows
    }
    missing = sorted(set(series_ids) - set(metadata))
    if missing:
        raise LookupError(f"macro_series metadata is missing for: {', '.join(missing)}")
    return metadata


def series_ids_from_payload(payload):
    """BLS timeseries API 응답에서 series id 목록을 추출한다."""
    series = payload.get("Results", {}).get("series", [])
    series_ids = [item.get("seriesID") for item in series if item.get("seriesID")]
    if not series_ids:
        raise ValueError("BLS raw payload has no Results.series entries")
    return series_ids


def records_from_timeseries_source(connection, source_object):
    """TIMESERIES raw 파일을 macro_* 적재용 records dict로 다시 변환한다."""
    payload = read_raw_payload(source_object)
    collected_at = datetime.now(timezone.utc)
    series_ids = series_ids_from_payload(payload)
    series_metadata = load_series_metadata(connection, series_ids)

    source_preview = {
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
    series_records = transform_macro_series(series_metadata, collected_at)
    observation_records = transform_observations(
        payload,
        collected_at,
        source_object["object_uri"],
    )
    status_records = transform_observation_statuses(
        payload,
        collected_at,
        source_object["object_uri"],
    )
    vintage_records = transform_observation_vintages(observation_records, collected_at)

    lineage_records = []
    lineage_records.extend(
        build_bls_lineage(
            series_records,
            source_preview,
            "macro_series",
            lambda record: {
                "provider": record["provider"],
                "external_series_id": record["external_series_id"],
            },
            collected_at,
        )
    )
    lineage_records.extend(
        build_bls_lineage(
            observation_records,
            source_preview,
            "macro_observation",
            lambda record: {
                "provider": record["provider"],
                "external_series_id": record["external_series_id"],
                "reference_period": record["reference_period"],
            },
            collected_at,
        )
    )
    lineage_records.extend(
        build_bls_lineage(
            status_records,
            source_preview,
            "macro_observation_status",
            lambda record: {
                "provider": record["provider"],
                "external_series_id": record["external_series_id"],
                "reference_period": record["reference_period"],
                "status": record["status"],
            },
            collected_at,
        )
    )
    lineage_records.extend(
        build_bls_lineage(
            vintage_records,
            source_preview,
            "macro_observation_vintage",
            lambda record: {
                "provider": record["provider"],
                "external_series_id": record["external_series_id"],
                "reference_period": record["reference_period"],
                "content_hash": record["content_hash"],
            },
            collected_at,
        )
    )

    return {
        "macro_series": series_records,
        "macro_release": [],
        "macro_observation": observation_records,
        "macro_observation_status": status_records,
        "macro_observation_vintage": vintage_records,
        "source_object": [source_preview],
        "record_lineage": lineage_records,
    }


def write_etl_previews(args, source_object_id, records):
    """ETL 변환 결과를 JSONL preview로 남긴다."""
    output_root = Path(args.output_dir) / f"source-object-{source_object_id}" / "tables"
    output_root.mkdir(parents=True, exist_ok=True)
    for table_name, rows in records.items():
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


def build_checkpoint(source_object, stats):
    """BLS ETL 결과 요약을 만든다."""
    return {
        "worker": "bls_macro_etl_worker",
        "mode": "ETL_ONLY",
        "source_object_id": source_object["id"],
        "storage_key": source_object.get("storage_key"),
        "object_uri": source_object.get("object_uri"),
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "stats": dict(sorted(stats.items())),
    }


def process_source_object(connection, source_object_id, args):
    """BLS TIMESERIES source_object 하나를 정규화 테이블에 적재한다."""
    source_object = load_source_object(connection, source_object_id)
    if source_object["provider"] != "BLS" or source_object["data_type"] != "TIMESERIES":
        raise ValueError(
            f"unsupported source_object for BLS ETL: "
            f"{source_object['provider']}/{source_object['data_type']}"
        )

    mark_source_object_status(connection, source_object_id, "PROCESSING")
    connection.commit()

    try:
        records = records_from_timeseries_source(connection, source_object)
        write_etl_previews(args, source_object_id, records)
        stats = apply_bls_macro_records(connection, records)
        checkpoint = build_checkpoint(source_object, stats)
        update_item_etl_checkpoint(connection, source_object, checkpoint)
        mark_source_object_status(connection, source_object_id, "PROCESSED")
        connection.commit()
        return checkpoint
    except Exception as error:
        connection.rollback()
        mark_source_object_status(connection, source_object_id, "FAILED", error_message=str(error))
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
                "retrying previously failed BLS raw event idempotency_key=%s",
                idempotency_key,
            )
        else:
            LOGGER.info(
                "duplicate BLS raw event skipped idempotency_key=%s previous_status=%s",
                idempotency_key,
                already_processed["status"],
            )
            return "duplicate"

    if source_object_completed_with_lineage(connection, source_object_id):
        LOGGER.info(
            "BLS source_object already processed; recording missing consumer success source_object_id=%s",
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
        LOGGER.exception("BLS raw ETL failed source_object_id=%s", source_object_id)
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
    """BLS ETL-only worker entrypoint."""
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
            "module": "workers.bls_macro_etl_worker",
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
                    "BLS raw message committed topic=%s partition=%s offset=%s result=%s",
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
