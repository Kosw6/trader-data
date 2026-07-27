import argparse
import json
import logging
import os
import time
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

from collectors.bls_client import BLSClient
from collectors.bls_collector import collect_bls
from workers.bls_macro_worker import (
    build_storage,
    load_series_metadata,
    parse_series_id,
    read_year_range,
)
from workers.db import connect_database
from workers.job_repository import claim_next_queued_item, complete_item, fail_item
from workers.raw_repository import report_raw_ready, upsert_collected_source_object


LOGGER = logging.getLogger("bls-macro-collector")


def parse_args():
    """BLS collector-only worker 실행 옵션을 읽는다."""
    parser = argparse.ArgumentParser(
        description=(
            "QUEUED BLS item 하나를 잡아 BLS API 원본을 저장하고 "
            "RAW_OBJECT_READY 이벤트 생성만 요청합니다."
        )
    )
    parser.add_argument("--job-type", default="BLS_MACRO_BACKFILL")
    parser.add_argument("--output-dir", default="pipeline_output/bls_macro_collector")
    parser.add_argument("--include-calendar", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--poll-interval-seconds", type=float, default=10.0)
    parser.add_argument(
        "--controller-url",
        default=os.getenv("TRADER_CONTROLLER_URL", "http://localhost:8090"),
    )
    parser.add_argument(
        "--admin-api-key",
        default=os.getenv("ADMIN_API_KEY", ""),
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def write_table_previews(storage, records):
    """collector가 만든 preview 테이블을 JSONL로 남겨 수동 검증을 쉽게 한다."""
    for table_name, table_records in records.items():
        path, count = storage.write_table(table_name, table_records)
        LOGGER.info("%s rows=%d path=%s", table_name, count, path)


def primary_timeseries_source(records):
    """BLS ETL 이벤트 대표 source_object로 TIMESERIES raw를 선택한다."""
    for source in records["source_object"]:
        if source["data_type"] == "TIMESERIES":
            return source
    raise LookupError("BLS TIMESERIES source_object was not produced")


def build_checkpoint(item, series_id, start_year, end_year, stats, raw_ready_event):
    """collector-only 처리 결과를 pipeline_job_item.checkpoint에 저장할 형태로 만든다."""
    return {
        "worker": "bls_macro_collector",
        "pipeline": {
            "worker": "bls_macro_collector",
            "mode": "COLLECT_ONLY",
            "stages": [
                {"name": "RESOLVE_TARGET"},
                {"name": "COLLECT_RAW"},
                {"name": "WRITE_RAW"},
                {"name": "INSERT_SOURCE_OBJECT"},
                {"name": "REPORT_RAW_READY"},
            ],
        },
        "job_id": item.job_id,
        "item_id": item.item_id,
        "item_key": item.item_key,
        "series_id": series_id,
        "start_year": start_year,
        "end_year": end_year,
        "stats": dict(sorted(stats.items())),
        "raw_ready_outbox_id": raw_ready_event.get("id") if raw_ready_event else None,
        "raw_ready_topic": raw_ready_event.get("topic") if raw_ready_event else None,
    }


def process_item(connection, item, args):
    """BLS item 하나를 수집 전용 흐름으로 처리한다."""
    series_id = parse_series_id(item.item_key)
    start_year, end_year = read_year_range(item.parameters)
    storage = build_storage(args.output_dir, item)
    series_metadata = load_series_metadata(connection, series_id)
    client = BLSClient(os.getenv("BLS_USER_AGENT", "TraderDataBot-KR/1.0"))

    records = collect_bls(
        client,
        storage,
        start_year,
        end_year,
        include_calendar=args.include_calendar,
        series_ids=[series_id],
        series_metadata=series_metadata,
    )
    write_table_previews(storage, records)

    stats = Counter()
    source_id_by_dedupe_key = {}
    with connection.cursor() as cursor:
        for source in records["source_object"]:
            source_id = upsert_collected_source_object(cursor, source)
            source_id_by_dedupe_key[source["dedupe_key"]] = source_id
            stats["source_objects_upserted"] += 1
    connection.commit()

    timeseries_source = primary_timeseries_source(records)
    raw_ready_event = report_raw_ready(
        args.controller_url,
        args.admin_api_key,
        source_id_by_dedupe_key[timeseries_source["dedupe_key"]],
    )
    stats["macro_series_preview"] = len(records["macro_series"])
    stats["macro_observation_preview"] = len(records["macro_observation"])
    stats["macro_observation_status_preview"] = len(records.get("macro_observation_status", []))
    stats["macro_vintage_preview"] = len(records.get("macro_observation_vintage", []))

    checkpoint = build_checkpoint(
        item,
        series_id,
        start_year,
        end_year,
        stats,
        raw_ready_event,
    )
    return complete_item(connection, item.item_id, "COLLECTED", checkpoint=checkpoint)


def print_claimed_item(item):
    """CLI에서 어떤 item을 잡았는지 바로 확인할 수 있게 출력한다."""
    print("claimed=true")
    print(
        json.dumps(
            {
                "item_id": item.item_id,
                "job_id": item.job_id,
                "item_key": item.item_key,
                "parameters": item.parameters,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


def run_once(connection, args):
    """QUEUED BLS item 하나를 collector-only 방식으로 처리한다."""
    item = claim_next_queued_item(
        connection,
        source="BLS",
        job_type=args.job_type.upper() if args.job_type else None,
    )
    if item is None:
        print("claimed=false")
        print("message=조건에 맞는 QUEUED BLS pipeline_job_item이 없습니다")
        return False

    print_claimed_item(item)
    try:
        next_job_status = process_item(connection, item, args)
        print("processed=true")
        print(f"next_job_status={next_job_status}")
    except Exception as error:
        LOGGER.exception("BLS collector item failed item_id=%s", item.item_id)
        connection.rollback()
        next_job_status = fail_item(
            connection,
            item.item_id,
            "BLS_COLLECTOR_FAILED",
            str(error),
            checkpoint={"worker": "bls_macro_collector", "item_key": item.item_key},
        )
        print("processed=false")
        print(f"next_job_status={next_job_status}")
        raise
    return True


def main():
    """BLS collector-only worker entrypoint."""
    load_dotenv()
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    connection = connect_database()
    try:
        if args.loop:
            while True:
                processed = run_once(connection, args)
                if not processed:
                    time.sleep(max(1.0, args.poll_interval_seconds))
            return
        run_once(connection, args)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
