import argparse
import json
import logging
import os
import signal
import threading

from dotenv import load_dotenv

from workers.db import connect_database
from workers.event_repository import find_processed_event, record_processed_event
from workers.job_repository import (
    TERMINAL_ITEM_STATUSES,
    claim_queued_item_by_id,
    get_item_status,
)
from workers.kafka_dispatcher import (
    current_event_status,
    dispatch_item,
    fail_claimed_item,
)
from workers.heartbeat import WorkerHeartbeatReporter


LOGGER = logging.getLogger("kafka-pipeline-consumer")


def parse_args():
    """Kafka consumer 실행에 필요한 topic, group, broker 정보를 CLI 또는 환경 변수로 받는다."""
    parser = argparse.ArgumentParser(
        description="Kafka JOB_ITEM_QUEUED 이벤트를 소비해 기존 trader-data worker로 처리합니다."
    )
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "trader.jobs.events"))
    parser.add_argument("--group-id", default=os.getenv("KAFKA_GROUP_ID", "trader-data-workers"))
    parser.add_argument(
        "--consumer-name",
        default=os.getenv("KAFKA_CONSUMER_NAME", "trader-data-worker"),
        help="processed_event 중복 방지에 사용할 consumer 이름입니다.",
    )
    parser.add_argument("--max-records", type=int, default=0, help="0이면 계속 실행합니다.")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def create_consumer(args):
    """kafka-python KafkaConsumer를 생성한다.

    enable_auto_commit=False로 두고, DB 처리와 processed_event 기록이 끝난 뒤에만
    offset을 commit한다. 이렇게 해야 worker 처리 중 죽었을 때 Kafka가 메시지를 다시 전달한다.
    """
    try:
        from kafka import KafkaConsumer
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "kafka-python이 필요합니다. trader-data에서 `pip install -r requirements.txt`를 실행하세요."
        ) from exc

    return KafkaConsumer(
        args.topic,
        bootstrap_servers=[broker.strip() for broker in args.brokers.split(",") if broker.strip()],
        group_id=args.group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=1,
        max_poll_interval_ms=900000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def process_message(connection, message, consumer_name):
    """Kafka 메시지 하나를 DB item 처리로 변환한다.

    처리 순서:
    1. idempotency_key가 processed_event에 있으면 중복으로 보고 건너뛴다.
    2. payload.itemId에 해당하는 pipeline_job_item을 QUEUED일 때만 RUNNING으로 선점한다.
    3. source에 맞는 기존 worker process_item을 호출한다.
    4. 최종 결과를 processed_event에 저장한다.
    """
    payload = message.value
    idempotency_key = read_required(payload, "idempotencyKey")
    item_id = int(read_required(payload, "itemId"))
    source = read_required(payload, "source").upper()
    job_type = read_required(payload, "jobType").upper()

    already_processed = find_processed_event(connection, consumer_name, idempotency_key)
    if already_processed:
        connection.rollback()  # End the duplicate-detection read transaction.
        LOGGER.info(
            "duplicate kafka event skipped idempotency_key=%s previous_status=%s",
            idempotency_key,
            already_processed["status"],
        )
        return "duplicate"

    item = claim_queued_item_by_id(connection, item_id, source=source, job_type=job_type)
    if item is None:
        return handle_unclaimable_item(
            connection,
            consumer_name,
            idempotency_key,
            message,
            item_id,
        )

    try:
        dispatch_item(connection, item)
        event_status, error_message = current_event_status(connection, item.item_id)
        record_processed_event(
            connection,
            consumer_name,
            idempotency_key,
            message.topic,
            message.partition,
            message.offset,
            event_status,
            error_message=error_message,
        )
    except Exception as exc:
        import psycopg2
        from collectors.s3_raw import RawStorageError
        if isinstance(exc, (RawStorageError, psycopg2.Error)):
            # Uncertain storage/SQL state requires review, never an offset commit
            # or a blind FAILED write over a possibly committed result.
            raise
        if item.source.upper() == "NEWS":
            # Expected provider/schema failures are finalized by news_collector. Unknown
            # failures or a disabled deployment must not acknowledge/overwrite its checkpoint.
            raise
        LOGGER.exception("kafka item processing failed item_id=%s", item.item_id)
        connection.rollback()
        try:
            fail_claimed_item(connection, item, exc)
        finally:
            record_processed_event(
                connection,
                consumer_name,
                idempotency_key,
                message.topic,
                message.partition,
                message.offset,
                "FAILED",
                error_message=str(exc),
            )
        return "failed"

    # Keep cleanup outside the collection error handler: a cleanup failure must
    # never overwrite a confirmed SUCCESS with FAILED.
    if event_status == "SUCCESS" and os.getenv("RAW_STORAGE_BACKEND", "LOCAL").upper() == "S3":
        from collectors.common import cleanup_verified_s3_previews
        cleanup_verified_s3_previews(item.item_id, item.job_id)
    return event_status.lower()


def handle_unclaimable_item(connection, consumer_name, idempotency_key, message, item_id):
    """메시지가 가리킨 item을 선점하지 못했을 때의 정책을 처리한다.

    이미 SUCCESS/SKIPPED/FAILED 같은 종료 상태라면 중복 메시지로 보고 processed_event에 SKIPPED를 남긴다.
    RUNNING/RETRY_WAIT처럼 아직 처리 중이거나 재시도 대기 상태라면 예외를 발생시켜 Kafka offset commit을 막는다.
    """
    status = get_item_status(connection, item_id)
    if status is None:
        record_processed_event(
            connection,
            consumer_name,
            idempotency_key,
            message.topic,
            message.partition,
            message.offset,
            "FAILED",
            error_message=f"pipeline_job_item id={item_id}를 찾을 수 없습니다.",
        )
        return "failed"

    if status["status"] in TERMINAL_ITEM_STATUSES:
        record_processed_event(
            connection,
            consumer_name,
            idempotency_key,
            message.topic,
            message.partition,
            message.offset,
            "SKIPPED",
            error_message=f"item이 이미 종료 상태입니다: {status['status']}",
        )
        return "skipped"

    raise RuntimeError(
        f"item id={item_id} 상태가 {status['status']}라 Kafka 메시지를 아직 commit하지 않습니다."
    )


def read_required(payload, key):
    """Kafka payload에서 필수 필드를 읽고 누락 시 명확한 오류를 낸다."""
    value = payload.get(key)
    if value in (None, ""):
        raise ValueError(f"Kafka payload에 필수 필드가 없습니다: {key}")
    return value


def consume(consumer, connection, heartbeat, args, stop):
    """One record at a time; commit only the record actually processed.

    SIGTERM stops admission, lets the current synchronous item finish and then
    returns. A hard kill still leaves its offset uncommitted for recovery.
    """
    from kafka import TopicPartition, OffsetAndMetadata
    processed = 0
    while not stop.is_set():
        batches = consumer.poll(timeout_ms=1000, max_records=1)
        for messages in batches.values():
            for message in messages:
                if stop.is_set():
                    return
                heartbeat.mark_busy(running_job_items=1)
                result = process_message(connection, message, args.consumer_name)
                consumer.commit({TopicPartition(message.topic, message.partition): OffsetAndMetadata(message.offset + 1, None)})
                processed += 1
                heartbeat.mark_idle(metadata={"lastResult": result, "lastCommittedOffset": message.offset})
                if args.max_records and processed >= args.max_records:
                    return


def main():
    """Kafka 메시지를 계속 소비하면서 trader-data worker 처리로 넘긴다."""
    load_dotenv()
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    consumer = create_consumer(args)
    connection = connect_database()
    heartbeat = WorkerHeartbeatReporter.from_env(
        args.consumer_name,
        metadata={
            "module": "workers.kafka_pipeline_consumer",
            "topic": args.topic,
            "groupId": args.group_id,
            "brokers": args.brokers,
        },
    )
    heartbeat.start()
    stop = threading.Event()
    old_handlers = {}
    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGTERM, signal.SIGINT):
            old_handlers[signum] = signal.signal(signum, lambda *unused: stop.set())
    failed = False
    try:
        consume(consumer, connection, heartbeat, args, stop)
    except Exception as exc:
        failed = True
        heartbeat.mark_error(metadata={"lastError": str(exc)})
        raise
    finally:
        heartbeat.stop(status="ERROR" if failed else "STOPPED")
        connection.close()
        consumer.close()
        for signum, old in old_handlers.items():
            signal.signal(signum, old)


if __name__ == "__main__":
    main()
