import argparse
import json
import logging
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

from pipeline.request_policy import RequestPolicyError

from workers.db import connect_database
from workers.job_repository import claim_next_queued_item, complete_item, fail_item
from workers.kis_price_worker import (
    apply_incremental_window,
    build_storage,
    load_asset,
    parse_asset_id,
    read_price_options,
    write_raw_pages,
)


LOGGER = logging.getLogger("kis-price-collector")


def parse_args():
    """KIS raw collector-only worker 실행 옵션을 읽는다."""
    parser = argparse.ArgumentParser(
        description=(
            "QUEUED KIS job item 하나를 잡아 KIS API 호출, raw 저장, "
            "source_object 기록, RAW_OBJECT_READY 보고까지만 수행합니다."
        )
    )
    parser.add_argument("--job-type", default="KIS_PRICE_BACKFILL")
    parser.add_argument("--output-dir", default="pipeline_output/kis_price_collector")
    parser.add_argument("--source", default="KIS")
    parser.add_argument("--interval", default="1D")
    parser.add_argument("--currency", default="USD")
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--request-interval", type=float, default=0.2)
    parser.add_argument("--request-policy", choices=("legacy", "bounded-v1", "coordinated-v1"), default="legacy",
                        help="Opt-in bounded HTTP policy; existing jobs keep legacy behavior.")
    parser.add_argument("--adjusted", action="store_true")
    parser.add_argument("--recovery-manifest")
    parser.add_argument("--recovery-manifest-sha256")
    parser.add_argument("--recovery-item-id", type=int)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--poll-interval-seconds", type=float, default=10.0)
    parser.add_argument(
        "--controller-url",
        default=os.getenv("TRADER_CONTROLLER_URL", "http://localhost:8090"),
        help="RAW_OBJECT_READY 보고를 받을 trader-controller base URL입니다.",
    )
    parser.add_argument(
        "--admin-api-key",
        default=os.getenv("ADMIN_API_KEY", ""),
        help="trader-controller 관리자 API Key입니다.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def upsert_collected_source_object(cursor, source):
    """raw 파일 메타데이터를 source_object에 COLLECTED 상태로 저장하고 id를 반환한다."""
    cursor.execute(
        """
        INSERT INTO source_object (
            provider, data_type, external_id, pipeline_job_item_id,
            storage_backend, storage_key, object_uri, object_size,
            content_hash, dedupe_key, collected_at, processing_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'COLLECTED')
        ON CONFLICT (dedupe_key)
        DO UPDATE SET
            pipeline_job_item_id = COALESCE(EXCLUDED.pipeline_job_item_id, source_object.pipeline_job_item_id),
            storage_backend = EXCLUDED.storage_backend,
            storage_key = EXCLUDED.storage_key,
            object_uri = EXCLUDED.object_uri,
            object_size = EXCLUDED.object_size,
            processing_status = CASE
                WHEN source_object.processing_status = 'PROCESSED' THEN source_object.processing_status
                ELSE 'COLLECTED'
            END
        RETURNING id
        """,
        (
            source["provider"],
            source["data_type"],
            source["external_id"],
            source.get("pipeline_job_item_id"),
            source.get("storage_backend", "LOCAL"),
            source["storage_key"],
            source["object_uri"],
            source["object_size"],
            source["content_hash"],
            source["dedupe_key"],
            source["collected_at"],
        ),
    )
    return cursor.fetchone()[0]


def report_raw_ready(controller_url, admin_api_key, source_object_id, timeout=10):
    """Go controller에 raw 저장 완료를 보고해 RAW_OBJECT_READY outbox 생성을 요청한다."""
    if not admin_api_key:
        raise RuntimeError("ADMIN_API_KEY is required to call trader-controller")

    url = f"{controller_url.rstrip('/')}/api/v1/admin/source-objects/{source_object_id}/raw-ready"
    response = requests.post(
        url,
        headers={"X-Admin-API-Key": admin_api_key},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def write_collector_previews(storage, source_object):
    """운영 확인용으로 collector가 만든 source_object preview만 JSONL로 남긴다."""
    storage.write_table("source_object", [source_object])


def build_collector_checkpoint(item, asset, options, stats, source_object, raw_ready_event):
    """job item에 남길 collector-only 처리 결과를 구성한다."""
    return {
        "worker": "kis_price_collector",
        "pipeline": {
            "worker": "kis_price_collector",
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
        "asset": asset,
        "start_date": options["start_date"].isoformat(),
        "end_date": options["end_date"].isoformat(),
        "requested_start_date": options["requested_start_date"].isoformat(),
        "requested_end_date": options["requested_end_date"].isoformat(),
        "collection_mode": options["collection_mode"],
        "latest_bar_date": options["latest_bar_date"].isoformat()
        if options.get("latest_bar_date")
        else None,
        "adjusted": options["adjusted"],
        "source": options["source"],
        "interval": options["interval"],
        "currency": options["currency"],
        "stats": dict(sorted(stats.items())),
        "source_object_id": stats.get("source_object_id"),
        "raw_object_uri": source_object.get("object_uri") if source_object else None,
        "storage_key": source_object.get("storage_key") if source_object else None,
        "raw_ready_outbox_id": raw_ready_event.get("id") if raw_ready_event else None,
        "raw_ready_topic": raw_ready_event.get("topic") if raw_ready_event else None,
    }


def process_item(connection, item, cli_args):
    """KIS item 하나를 raw 수집까지만 처리하고 RAW_OBJECT_READY 이벤트 생성을 요청한다."""
    if getattr(cli_args, "request_policy", "legacy") == "coordinated-v1":
        from workers.coordinated_collector import process
        return process(connection, item, cli_args, "KIS")
    from collectors.policy_clients import kis_client, policy_checkpoint

    asset_id = parse_asset_id(item.item_key)
    asset = load_asset(connection, asset_id)
    if asset is None:
        return complete_item(
            connection,
            item.item_id,
            "SKIPPED",
            checkpoint={"worker": "kis_price_collector", "item_key": item.item_key},
            error_code="KIS_ASSET_NOT_FOUND",
            error_message="활성 asset 또는 현재 KIS provider symbol이 없어 raw 수집을 건너뜁니다.",
        )

    options = read_price_options(item.parameters or {}, cli_args)
    options = apply_incremental_window(connection, asset, options)
    if options["start_date"] > options["end_date"]:
        stats = Counter({"skipped_already_up_to_date": 1})
        checkpoint = build_collector_checkpoint(item, asset, options, stats, None, None)
        return complete_item(
            connection,
            item.item_id,
            "SKIPPED",
            checkpoint=checkpoint,
            error_code="KIS_ALREADY_UP_TO_DATE",
            error_message="DB 최신 price_bar가 요청 종료일 이상이라 raw 수집을 생략했습니다.",
        )

    storage = build_storage(cli_args.output_dir, item)
    client = kis_client(getattr(cli_args, "request_policy", "legacy"),
                        min_request_interval=options["request_interval"])
    collected_at = datetime.now(timezone.utc)

    try:
        result = client.get_daily_prices(
            exchange_code=asset["provider_exchange_code"],
            symbol=asset["provider_symbol"],
            start_date=options["start_date"],
            end_date=options["end_date"],
            adjusted=options["adjusted"],
            max_pages=options["max_pages"],
        )
        request_checkpoint = policy_checkpoint(client)
    finally:
        client.session.close()
    source_object = write_raw_pages(storage, asset, options, result, collected_at)
    write_collector_previews(storage, source_object)

    stats = Counter()
    stats["fetched_rows"] = len(result["bars"])
    stats["raw_pages"] = len(result["raw_pages"])
    with connection.cursor() as cursor:
        source_object_id = upsert_collected_source_object(cursor, source_object)
    connection.commit()
    stats["source_objects_upserted"] = 1
    stats["source_object_id"] = source_object_id

    raw_ready_event = report_raw_ready(
        cli_args.controller_url,
        cli_args.admin_api_key,
        source_object_id,
    )
    checkpoint = build_collector_checkpoint(
        item,
        asset,
        options,
        stats,
        source_object,
        raw_ready_event,
    )
    checkpoint.update(request_checkpoint)
    return complete_item(connection, item.item_id, "COLLECTED", checkpoint=checkpoint)


def print_claimed_item(item):
    """worker가 어떤 item을 잡았는지 CLI에서 바로 확인할 수 있게 출력한다."""
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
    """QUEUED KIS item 하나를 collector-only 방식으로 처리한다."""
    coordinated = getattr(args, "request_policy", "legacy") == "coordinated-v1"
    if coordinated:
        from workers.coordinated_collector import claim

        item = claim(connection, args, "KIS")
    else:
        item = claim_next_queued_item(
            connection,
            source="KIS",
            job_type=args.job_type.upper() if args.job_type else None,
        )
    if item is None:
        print("claimed=false")
        print("message=조건에 맞는 QUEUED KIS pipeline_job_item이 없습니다")
        return False

    if coordinated:
        print(f"claimed=true item_id={item.item_id} mode=coordinated-v1")
    else:
        print_claimed_item(item)
    try:
        next_job_status = process_item(connection, item, args)
        print("processed=true")
        print(f"next_job_status={next_job_status}")
    except Exception as error:
        if coordinated:
            from pipeline.durable_requests import CoordinationError
            connection.rollback()
            LOGGER.error("Coordinated KIS collection held item_id=%s; no unfenced failure write", item.item_id)
            raise CoordinationError(error.code if isinstance(error, CoordinationError) else "review_required") from None
        LOGGER.exception("KIS collector item failed item_id=%s", item.item_id)
        connection.rollback()
        checkpoint = {"worker": "kis_price_collector", "item_key": item.item_key}
        if isinstance(error, RequestPolicyError):
            checkpoint["requestPolicy"] = error.metadata()
        next_job_status = fail_item(
            connection,
            item.item_id,
            "KIS_COLLECTOR_FAILED",
            str(error),
            checkpoint=checkpoint,
        )
        print("processed=false")
        print(f"next_job_status={next_job_status}")
        raise
    return True


def main():
    """KIS collector-only worker entrypoint."""
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
