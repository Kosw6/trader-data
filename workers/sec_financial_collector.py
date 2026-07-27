import argparse
import json
import logging
import os
import time
from collections import Counter

from dotenv import load_dotenv

from backfills.sec_financial.backfill import (
    collect_sec_financial_payloads,
    require_user_agent,
)
from collectors.sec_client import SECClient
from workers.db import connect_database
from workers.job_repository import claim_next_queued_item, complete_item, fail_item
from workers.raw_repository import report_raw_ready, upsert_collected_source_object
from workers.sec_financial_worker import (
    build_backfill_args,
    build_storage,
    load_single_target,
    write_table_previews,
)


LOGGER = logging.getLogger("sec-financial-collector")


def parse_args():
    """SEC collector-only worker 실행 옵션을 읽는다."""
    parser = argparse.ArgumentParser(
        description=(
            "QUEUED SEC item 하나를 잡아 submissions/companyfacts raw를 저장하고 "
            "대표 source_object에 대해 RAW_OBJECT_READY 이벤트를 요청합니다."
        )
    )
    parser.add_argument("--job-type", default="SEC_FINANCIAL_BACKFILL")
    parser.add_argument("--output-dir", default="pipeline_output/sec_financial_collector")
    parser.add_argument("--include-history", action="store_true")
    parser.add_argument("--all-facts", action="store_true")
    parser.add_argument("--request-interval", type=float, default=1.0)
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


def primary_sec_source(records):
    """SEC ETL 이벤트 대표 source_object를 고른다.

    financial_fact 적재에는 COMPANY_FACTS가 핵심이므로 우선 선택한다.
    companyfacts가 없는 기업은 filing만이라도 처리할 수 있게 SUBMISSIONS를 선택한다.
    """
    for source in records["source_object"]:
        if source["data_type"] == "COMPANY_FACTS":
            return source
    for source in records["source_object"]:
        if source["data_type"] == "SUBMISSIONS":
            return source
    raise LookupError("SEC source_object was not produced")


def build_checkpoint(item, target, backfill_args, stats, raw_ready_event):
    """collector-only 처리 결과를 checkpoint JSON으로 만든다."""
    return {
        "worker": "sec_financial_collector",
        "pipeline": {
            "worker": "sec_financial_collector",
            "mode": "COLLECT_ONLY",
            "stages": [
                {"name": "RESOLVE_TARGET"},
                {"name": "COLLECT_RAW"},
                {"name": "WRITE_RAW"},
                {"name": "INSERT_SOURCE_OBJECTS"},
                {"name": "REPORT_RAW_READY"},
            ],
        },
        "job_id": item.job_id,
        "item_id": item.item_id,
        "item_key": item.item_key,
        "ticker": target.get("ticker"),
        "cik": target.get("cik"),
        "company_id": target.get("company_id"),
        "include_history": backfill_args.include_history,
        "all_facts": backfill_args.all_facts,
        "request_interval": backfill_args.request_interval,
        "stats": dict(sorted(stats.items())),
        "raw_ready_outbox_id": raw_ready_event.get("id") if raw_ready_event else None,
        "raw_ready_topic": raw_ready_event.get("topic") if raw_ready_event else None,
    }


def process_item(connection, item, cli_args):
    """SEC item 하나를 수집 전용 흐름으로 처리한다."""
    target, target_ref = load_single_target(connection, item.item_key)
    if target is None:
        return complete_item(
            connection,
            item.item_id,
            "SKIPPED",
            checkpoint={
                "worker": "sec_financial_collector",
                "item_key": item.item_key,
                "target": target_ref,
            },
            error_code="SEC_TARGET_NOT_FOUND",
            error_message="DB에 연결된 company/asset 대상이 없어 SEC raw 수집을 건너뜁니다.",
        )

    backfill_args = build_backfill_args(item, cli_args)
    storage = build_storage(backfill_args.output_dir, item)
    client = SECClient(
        require_user_agent(),
        min_request_interval=backfill_args.request_interval,
    )

    from datetime import datetime, timezone

    collected_at = datetime.now(timezone.utc)
    records = collect_sec_financial_payloads(
        client,
        storage,
        target,
        backfill_args,
        collected_at,
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

    representative = primary_sec_source(records)
    raw_ready_event = report_raw_ready(
        cli_args.controller_url,
        cli_args.admin_api_key,
        source_id_by_dedupe_key[representative["dedupe_key"]],
    )

    stats["filing_preview"] = len(records["filing"])
    stats["financial_fact_preview"] = len(records["financial_fact"])
    stats["company_facts_missing"] = int(bool(records.get("company_facts_missing")))
    checkpoint = build_checkpoint(item, target, backfill_args, stats, raw_ready_event)
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
    """QUEUED SEC item 하나를 collector-only 방식으로 처리한다."""
    item = claim_next_queued_item(
        connection,
        source="SEC",
        job_type=args.job_type.upper() if args.job_type else None,
    )
    if item is None:
        print("claimed=false")
        print("message=조건에 맞는 QUEUED SEC pipeline_job_item이 없습니다")
        return False

    print_claimed_item(item)
    try:
        next_job_status = process_item(connection, item, args)
        print("processed=true")
        print(f"next_job_status={next_job_status}")
    except Exception as error:
        LOGGER.exception("SEC collector item failed item_id=%s", item.item_id)
        connection.rollback()
        next_job_status = fail_item(
            connection,
            item.item_id,
            "SEC_COLLECTOR_FAILED",
            str(error),
            checkpoint={"worker": "sec_financial_collector", "item_key": item.item_key},
        )
        print("processed=false")
        print(f"next_job_status={next_job_status}")
        raise
    return True


def main():
    """SEC collector-only worker entrypoint."""
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
