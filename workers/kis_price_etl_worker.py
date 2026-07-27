import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

from dotenv import load_dotenv
from psycopg2.extras import Json

from collectors.kis_client import normalize_daily_price
from workers.db import connect_database
from workers.event_repository import find_processed_event, record_processed_event
from workers.heartbeat import WorkerHeartbeatReporter
from workers.raw_repository import source_object_completed_with_lineage
from workers.kis_price_worker import (
    build_price_rows,
    upsert_price_bar_lineage,
)
from backfills.price_bar_invalid_repair import (
    DEFAULT_INTERVAL,
    DEFAULT_SOURCE,
    upsert_price_bars,
)


LOGGER = logging.getLogger("kis-price-etl-worker")


def parse_args():
    """KIS raw ETL-only worker 실행 옵션을 읽는다."""
    parser = argparse.ArgumentParser(
        description=(
            "KIS RAW_OBJECT_READY 이벤트 또는 source_object_id를 받아 raw 파일을 읽고 "
            "price_bar와 record_lineage만 적재합니다."
        )
    )
    parser.add_argument("--source-object-id", type=int, help="Kafka 없이 특정 source_object만 처리합니다.")
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_RAW_KIS_TOPIC", "trader.raw.kis.ready"))
    parser.add_argument("--group-id", default=os.getenv("KAFKA_RAW_KIS_GROUP_ID", "trader-data-kis-etl"))
    parser.add_argument(
        "--consumer-name",
        default=os.getenv("KAFKA_RAW_KIS_CONSUMER_NAME", "kis-price-etl-worker"),
    )
    parser.add_argument("--max-records", type=int, default=0, help="0이면 계속 소비합니다.")
    parser.add_argument("--output-dir", default="pipeline_output/kis_price_etl_worker")
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--interval", default=DEFAULT_INTERVAL)
    parser.add_argument("--currency", default="USD")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def create_consumer(args):
    """RAW_OBJECT_READY 전용 Kafka consumer를 생성한다."""
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


def load_source_object(connection, source_object_id):
    """ETL 대상 source_object와 연결된 job/item 정보를 조회한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                so.id,
                so.provider,
                so.data_type,
                so.external_id,
                so.pipeline_job_item_id,
                so.storage_backend,
                so.storage_key,
                so.object_uri,
                so.content_hash,
                so.processing_status,
                so.collected_at,
                i.item_key,
                j.id,
                j.job_type,
                j.source,
                j.parameters
            FROM source_object so
            LEFT JOIN pipeline_job_item i ON i.id = so.pipeline_job_item_id
            LEFT JOIN pipeline_job j ON j.id = i.job_id
            WHERE so.id = %s
            """,
            (source_object_id,),
        )
        row = cursor.fetchone()
    if row is None:
        raise LookupError(f"source_object id={source_object_id} not found")
    return {
        "id": row[0],
        "provider": row[1],
        "data_type": row[2],
        "external_id": row[3],
        "pipeline_job_item_id": row[4],
        "storage_backend": row[5],
        "storage_key": row[6],
        "object_uri": row[7],
        "content_hash": row[8],
        "processing_status": row[9],
        "collected_at": row[10],
        "item_key": row[11],
        "job_id": row[12],
        "job_type": row[13],
        "job_source": row[14],
        "parameters": row[15] or {},
    }


def mark_source_object_status(connection, source_object_id, status, error_message=None):
    """source_object 처리 상태와 처리 시각을 갱신한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE source_object
            SET
                processing_status = %s,
                processing_started_at = CASE
                    WHEN %s = 'PROCESSING' THEN COALESCE(processing_started_at, CURRENT_TIMESTAMP)
                    ELSE processing_started_at
                END,
                processing_finished_at = CASE
                    WHEN %s IN ('PROCESSED', 'FAILED') THEN CURRENT_TIMESTAMP
                    WHEN %s = 'PROCESSING' THEN NULL
                    ELSE processing_finished_at
                END,
                processing_error_message = CASE
                    WHEN %s = 'FAILED' THEN %s
                    WHEN %s IN ('PROCESSING', 'PROCESSED') THEN NULL
                    ELSE processing_error_message
                END,
                processing_attempt_count = CASE
                    WHEN %s = 'PROCESSING' THEN processing_attempt_count + 1
                    ELSE processing_attempt_count
                END
            WHERE id = %s
            """,
            (
                status,
                status,
                status,
                status,
                status,
                error_message,
                status,
                status,
                source_object_id,
            ),
        )


def update_item_etl_checkpoint(connection, source_object, checkpoint):
    """수집 item checkpoint에 ETL 요약을 덧붙인다. item 상태는 COLLECTED로 유지한다."""
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


def local_path_from_source_object(source_object):
    """source_object의 URI 또는 storage_key를 로컬 파일 경로로 변환한다."""
    if source_object["storage_backend"] != "LOCAL":
        raise NotImplementedError("S3 raw read is not implemented in this local PoC")

    uri = source_object.get("object_uri") or ""
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path.lstrip("/")) if os.name == "nt" else unquote(parsed.path))
    if uri:
        return Path(uri)

    base_dir = os.getenv("RAW_STORAGE_LOCAL_ROOT", "pipeline_output")
    return Path(base_dir) / source_object["storage_key"]


def read_raw_payload(source_object):
    """raw JSON 파일을 읽어 payload dict로 반환한다."""
    path = local_path_from_source_object(source_object)
    if not path.exists():
        raise FileNotFoundError(f"raw file does not exist: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def bars_from_raw_payload(payload):
    """KIS raw pages의 output2 배열을 price_bar 변환용 bar 목록으로 복원한다."""
    start_date = datetime.fromisoformat(payload["start_date"]).date()
    end_date = datetime.fromisoformat(payload["end_date"]).date()
    bars_by_date = {}
    for page in payload.get("pages", []):
        raw_payload = page.get("payload") or {}
        for item in raw_payload.get("output2", []):
            if not item.get("xymd"):
                continue
            bar = normalize_daily_price(item)
            if start_date <= bar["date"] <= end_date:
                bars_by_date[bar["date"]] = bar
    return [bars_by_date[key] for key in sorted(bars_by_date)]


def asset_from_raw_payload(payload):
    """raw payload에 저장된 asset 식별 정보를 ETL용 asset dict로 만든다."""
    return {
        "asset_id": int(payload["asset_id"]),
        "ticker": payload.get("ticker"),
        "exchange": payload.get("exchange"),
        "provider_exchange_code": payload.get("provider_exchange_code"),
        "provider_symbol": payload.get("provider_symbol"),
    }


def etl_options(source_object, payload, args):
    """job parameters와 CLI 기본값을 합쳐 ETL 적재 옵션을 만든다."""
    parameters = source_object.get("parameters") or {}
    return {
        "source": str(parameters.get("source") or args.source).strip().upper(),
        "interval": str(parameters.get("interval") or args.interval).strip().upper(),
        "currency": str(parameters.get("currency") or args.currency).strip().upper(),
        "adjusted": bool(payload.get("adjusted")),
    }


def write_etl_previews(args, source_object_id, source_object, rows, statuses):
    """ETL 결과를 로컬 JSONL preview로 남겨 디버깅과 운영 확인을 쉽게 한다."""
    output_root = Path(args.output_dir) / f"source-object-{source_object_id}" / "tables"
    output_root.mkdir(parents=True, exist_ok=True)
    records = {
        "source_object": [source_object],
        "price_bar": [
            {
                **row,
                "trade_date": row["trade_date"].isoformat(),
                "open": str(row["open"]),
                "high": str(row["high"]),
                "low": str(row["low"]),
                "close": str(row["close"]),
                "volume": str(row["volume"]) if row.get("volume") is not None else None,
            }
            for row in rows
        ],
        "price_bar_fetch_status": statuses,
    }
    for table_name, table_rows in records.items():
        path = output_root / f"{table_name}.jsonl"
        with path.open("w", encoding="utf-8", newline="\n") as output:
            for record in table_rows:
                output.write(json.dumps(record, ensure_ascii=False, default=str))
                output.write("\n")


def build_etl_checkpoint(source_object, stats):
    """source_object와 수집 item에 남길 ETL 처리 요약을 만든다."""
    return {
        "worker": "kis_price_etl_worker",
        "mode": "ETL_ONLY",
        "source_object_id": source_object["id"],
        "storage_key": source_object.get("storage_key"),
        "object_uri": source_object.get("object_uri"),
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "stats": dict(sorted(stats.items())),
    }


def process_source_object(connection, source_object_id, args):
    """source_object 하나를 읽어 KIS price_bar 정규화 적재를 수행한다."""
    source_object = load_source_object(connection, source_object_id)
    if source_object["provider"] != "KIS" or source_object["data_type"] != "PRICE_BAR":
        raise ValueError(
            f"unsupported source_object for KIS ETL: "
            f"{source_object['provider']}/{source_object['data_type']}"
        )

    mark_source_object_status(connection, source_object_id, "PROCESSING")
    connection.commit()

    stats = {"source_object_id": source_object_id}
    try:
        payload = read_raw_payload(source_object)
        asset = asset_from_raw_payload(payload)
        options = etl_options(source_object, payload, args)
        bars = bars_from_raw_payload(payload)
        collected_at = datetime.now(timezone.utc)
        rows, statuses = build_price_rows(asset, bars, options, collected_at)

        stats["fetched_rows"] = len(bars)
        stats["valid_rows"] = len(rows)
        stats["invalid_rows"] = sum(1 for status in statuses if status["status"] == "INVALID")
        write_etl_previews(args, source_object_id, source_object, rows, statuses)

        upserted_bars = []
        if rows:
            upserted_bars = upsert_price_bars(
                connection,
                rows,
                options["source"],
                options["interval"],
                options["currency"],
                return_records=True,
            )
            with connection.cursor() as cursor:
                stats["lineage_upserted"] = upsert_price_bar_lineage(
                    cursor,
                    source_object_id,
                    upserted_bars,
                    collected_at,
                )
        else:
            stats["lineage_upserted"] = 0

        stats["affected_rows"] = len(upserted_bars)
        checkpoint = build_etl_checkpoint(source_object, stats)
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
                "retrying previously failed raw event idempotency_key=%s",
                idempotency_key,
            )
        else:
            LOGGER.info(
                "duplicate raw event skipped idempotency_key=%s previous_status=%s",
                idempotency_key,
                already_processed["status"],
            )
            return "duplicate"

    if source_object_completed_with_lineage(connection, source_object_id):
        LOGGER.info(
            "KIS source_object already processed; recording missing consumer success source_object_id=%s",
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
        LOGGER.exception("KIS raw ETL failed source_object_id=%s", source_object_id)
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
    """KIS ETL-only worker entrypoint."""
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
            "module": "workers.kis_price_etl_worker",
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
                    "raw message committed topic=%s partition=%s offset=%s result=%s",
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
