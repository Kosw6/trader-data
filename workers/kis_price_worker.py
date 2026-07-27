import argparse
import json
import logging
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from backfills.price_bar_invalid_repair import (
    DEFAULT_INTERVAL,
    DEFAULT_SOURCE,
    invalid_reasons,
    upsert_price_bars,
)
from collectors.common import (
    LocalPipelineStorage,
    build_target_record_key,
    content_hash,
)
from collectors.kis_client import KISClient
from pipeline.stages import PipelineMode, PipelineStage
from workers.db import connect_database
from workers.job_repository import claim_next_queued_item, complete_item, fail_item


LOGGER = logging.getLogger("kis-price-worker")


def parse_args():
    """KIS price worker 실행 범위와 polling 방식을 CLI 인자로 받는다."""
    parser = argparse.ArgumentParser(
        description="QUEUED KIS pipeline_job_item 하나를 선점해 KIS 일봉 수집과 price_bar 적재를 수행합니다."
    )
    parser.add_argument(
        "--job-type",
        default="KIS_PRICE_BACKFILL",
        help="처리할 pipeline_job.job_type 필터입니다.",
    )
    parser.add_argument(
        "--output-dir",
        default="pipeline_output/kis_price_worker",
        help="worker가 보존할 KIS raw 원본과 table preview 경로입니다.",
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help="price_bar.source 값입니다.",
    )
    parser.add_argument(
        "--interval",
        default=DEFAULT_INTERVAL,
        help="price_bar.interval 값입니다.",
    )
    parser.add_argument(
        "--currency",
        default="USD",
        help="Job parameters에 currency가 없을 때 사용할 기본 통화입니다.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=100,
        help="KIS pagination 최대 페이지 수입니다.",
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=0.2,
        help="KIS 요청 사이 최소 대기 시간입니다.",
    )
    parser.add_argument(
        "--adjusted",
        action="store_true",
        help="Job parameters에 adjusted가 없을 때 사용할 기본 수정주가 조회 여부입니다.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="QUEUED item이 없을 때 종료하지 않고 주기적으로 다시 확인합니다.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=10.0,
        help="--loop 모드에서 다음 item을 다시 확인하기 전 대기 시간입니다.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def parse_asset_id(item_key):
    """KIS:{asset_id} item key에서 DB asset id를 추출한다."""
    prefix = "KIS:"
    if not item_key.upper().startswith(prefix):
        raise ValueError(f"KIS item_key는 {prefix} 로 시작해야 합니다: {item_key}")
    raw_value = item_key[len(prefix):].strip()
    if not raw_value.isdigit():
        raise ValueError(f"KIS item_key에는 숫자 asset_id가 필요합니다: {item_key}")
    return int(raw_value)


def parse_date_value(value, fallback):
    """YYYY-MM-DD 문자열을 date로 변환하고 값이 없으면 fallback을 사용한다."""
    if value in (None, ""):
        return fallback
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def read_bool(parameters, key, default):
    """pipeline_job.parameters의 boolean 옵션을 안전하게 읽는다."""
    value = parameters.get(key)
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def read_float(parameters, key, default):
    """pipeline_job.parameters의 숫자 옵션을 안전하게 읽는다."""
    value = parameters.get(key)
    if value in (None, ""):
        return float(default)
    return float(value)


def read_int(parameters, key, default):
    """pipeline_job.parameters의 정수 옵션을 안전하게 읽는다."""
    value = parameters.get(key)
    if value in (None, ""):
        return int(default)
    return int(value)


def read_collection_mode(parameters):
    """수집 모드를 읽는다.

    BACKFILL은 사용자가 지정한 startDate/endDate를 그대로 요청한다.
    INCREMENTAL은 DB의 최신 price_bar 다음 날짜부터만 KIS에 요청한다.
    """
    value = parameters.get("mode") or parameters.get("collectionMode") or "BACKFILL"
    return str(value).strip().upper()


def read_price_options(parameters, cli_args):
    """Job parameters와 CLI 기본값을 합쳐 KIS 조회 옵션을 만든다."""
    today = datetime.now(timezone.utc).date()
    start_date = parse_date_value(parameters.get("startDate"), today)
    end_date = parse_date_value(parameters.get("endDate"), today)
    if end_date < start_date:
        raise ValueError(f"endDate는 startDate보다 빠를 수 없습니다: {start_date}>{end_date}")

    return {
        "start_date": start_date,
        "end_date": end_date,
        "adjusted": read_bool(parameters, "adjusted", cli_args.adjusted),
        "max_pages": read_int(parameters, "maxPages", cli_args.max_pages),
        "request_interval": read_float(parameters, "requestInterval", cli_args.request_interval),
        "source": str(parameters.get("source") or cli_args.source).strip().upper(),
        "interval": str(parameters.get("interval") or cli_args.interval).strip().upper(),
        "currency": str(parameters.get("currency") or cli_args.currency).strip().upper(),
        "collection_mode": read_collection_mode(parameters),
        "requested_start_date": start_date,
        "requested_end_date": end_date,
        "latest_bar_date": None,
    }


def load_asset(connection, asset_id):
    """DB asset id로 현재 KIS provider symbol을 조회한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                a.id,
                a.ticker,
                a.name,
                a.asset_type,
                a.exchange,
                a.currency,
                aps.provider_exchange_code,
                aps.provider_symbol
            FROM asset a
            JOIN asset_provider_symbol aps
              ON aps.asset_id = a.id
             AND aps.provider = 'KIS'
             AND aps.is_current = TRUE
            WHERE a.id = %s
              AND a.active = TRUE
            """,
            (asset_id,),
        )
        row = cursor.fetchone()

    if row is None:
        return None
    return {
        "asset_id": row[0],
        "ticker": row[1],
        "name": row[2],
        "asset_type": row[3],
        "exchange": row[4],
        "currency": row[5],
        "provider_exchange_code": row[6],
        "provider_symbol": row[7],
    }


def load_latest_price_bar_date(connection, asset_id, source, interval):
    """asset/source/interval 기준으로 DB에 저장된 가장 최신 일봉 날짜를 조회한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT MAX(bar_time)::date
            FROM price_bar
            WHERE asset_id = %s
              AND source = %s
              AND "interval" = %s
            """,
            (asset_id, source, interval),
        )
        row = cursor.fetchone()
    return row[0] if row else None


def apply_incremental_window(connection, asset, options):
    """INCREMENTAL 모드일 때 외부 API 요청 시작일을 최신 적재일 다음 날로 좁힌다.

    기존 BACKFILL 모드는 그대로 둔다. 이렇게 해야 관리자 페이지에서
    '전체 백필'과 '최신분만 수집'을 선택적으로 실행할 수 있다.
    """
    if options["collection_mode"] not in {"INCREMENTAL", "DAILY_INCREMENTAL", "LATEST_ONLY"}:
        return options

    latest_date = load_latest_price_bar_date(
        connection,
        asset["asset_id"],
        options["source"],
        options["interval"],
    )
    options["latest_bar_date"] = latest_date
    if latest_date is None:
        return options

    next_start = latest_date + timedelta(days=1)
    if next_start > options["start_date"]:
        options["start_date"] = next_start
    return options


def build_storage(root, item):
    """Job과 item 단위로 raw/table 출력 경로가 겹치지 않도록 storage를 만든다."""
    return LocalPipelineStorage(
        Path(root) / f"job-{item.job_id}" / f"item-{item.item_id}",
        pipeline_job_item_id=item.item_id,
    )


def write_raw_pages(storage, asset, options, result, collected_at):
    """KIS 원본 페이지 묶음을 raw JSON으로 보존하고 source_object preview를 반환한다."""
    payload = {
        "provider": "KIS",
        "asset_id": asset["asset_id"],
        "ticker": asset["ticker"],
        "exchange": asset["exchange"],
        "provider_exchange_code": asset["provider_exchange_code"],
        "provider_symbol": asset["provider_symbol"],
        "start_date": options["start_date"].isoformat(),
        "end_date": options["end_date"].isoformat(),
        "adjusted": options["adjusted"],
        "collected_at": collected_at.isoformat(),
        "pages": result["raw_pages"],
    }
    return storage.write_raw_json(
        "KIS",
        "PRICE_BAR",
        f"{asset['asset_id']}-{asset['provider_symbol']}",
        payload,
        collected_at,
    )


def build_price_rows(asset, bars, options, collected_at):
    """KIS 일봉을 price_bar UPSERT용 row와 검증 상태로 변환한다."""
    rows = []
    statuses = []
    for bar in bars:
        reasons = invalid_reasons(
            bar.get("open"),
            bar.get("high"),
            bar.get("low"),
            bar.get("close"),
            bar.get("volume"),
        )
        if reasons:
            statuses.append({
                "trade_date": bar["date"].isoformat(),
                "status": "INVALID",
                "reasons": list(reasons),
            })
            continue

        stable_payload = {
            "asset_id": asset["asset_id"],
            "source": options["source"],
            "interval": options["interval"],
            "bar_time": bar["date"].isoformat(),
            "open": str(bar["open"]),
            "high": str(bar["high"]),
            "low": str(bar["low"]),
            "close": str(bar["close"]),
            "volume": str(bar["volume"]) if bar["volume"] is not None else None,
            "currency": options["currency"],
        }
        rows.append({
            "asset_id": asset["asset_id"],
            "trade_date": bar["date"],
            "open": bar["open"],
            "high": bar["high"],
            "low": bar["low"],
            "close": bar["close"],
            "volume": bar["volume"],
            "collected_at": collected_at,
            "content_hash": content_hash(stable_payload),
        })
        statuses.append({
            "trade_date": bar["date"].isoformat(),
            "status": "VALID",
            "reasons": [],
        })
    return rows, statuses


def write_table_previews(storage, source_object, rows, statuses):
    """worker 처리 결과를 사람이 확인할 수 있도록 JSONL preview로 남긴다."""
    preview_rows = [
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
    ]
    storage.write_table("source_object", [source_object])
    storage.write_table("price_bar", preview_rows)
    storage.write_table("price_bar_fetch_status", statuses)


def upsert_source_object(cursor, source):
    """KIS raw object metadata를 source_object에 저장하고 id를 반환한다."""
    cursor.execute(
        """
        INSERT INTO source_object (
            provider, data_type, external_id, pipeline_job_item_id,
            storage_backend, storage_key, object_uri, object_size,
            content_hash, dedupe_key, collected_at, processing_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PROCESSED')
        ON CONFLICT (dedupe_key)
        DO UPDATE SET
            pipeline_job_item_id = COALESCE(EXCLUDED.pipeline_job_item_id, source_object.pipeline_job_item_id),
            storage_backend = EXCLUDED.storage_backend,
            storage_key = EXCLUDED.storage_key,
            object_uri = EXCLUDED.object_uri,
            object_size = EXCLUDED.object_size,
            processing_status = 'PROCESSED'
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


def price_bar_target_key(bar_record):
    """price_bar의 DB PK가 달라져도 같은 봉을 식별할 수 있는 안정 키를 만든다."""
    return build_target_record_key(
        "price_bar",
        {
            "asset_id": bar_record["asset_id"],
            "source": bar_record["source"],
            "interval": bar_record["interval"],
            "bar_time": bar_record["bar_time"].isoformat(),
        },
    )


def upsert_price_bar_lineage(cursor, source_object_id, upserted_bars, processed_at):
    """KIS source_object와 upsert된 price_bar row를 record_lineage로 연결한다."""
    if not upserted_bars:
        return 0

    from psycopg2.extras import execute_values

    rows = [
        (
            source_object_id,
            "price_bar",
            bar["id"],
            price_bar_target_key(bar),
            "kis_price_worker",
            "v1",
            bar.get("content_hash"),
            processed_at,
        )
        for bar in upserted_bars
    ]
    execute_values(
        cursor,
        """
        INSERT INTO record_lineage (
            source_object_id, target_table, target_record_id, target_record_key,
            transformer_name, transformer_version, output_hash, processed_at
        ) VALUES %s
        ON CONFLICT (
            source_object_id,
            target_table,
            target_record_key,
            transformer_name,
            transformer_version
        )
        DO UPDATE SET
            target_record_id = EXCLUDED.target_record_id,
            output_hash = EXCLUDED.output_hash,
            processed_at = EXCLUDED.processed_at
        """,
        rows,
        page_size=500,
    )
    return len(rows)


def build_checkpoint(item, asset, options, stats, source_object):
    """완료된 item의 재처리 기준과 결과 요약을 checkpoint JSON으로 구성한다."""
    return {
        "worker": "kis_price_worker",
        "pipeline": {
            "worker": "kis_price_worker",
            "mode": PipelineMode.COMBINED,
            "stages": [
                {"name": PipelineStage.RESOLVE_TARGET},
                {"name": PipelineStage.COLLECT_RAW},
                {"name": PipelineStage.WRITE_RAW_PREVIEW},
                {"name": PipelineStage.TRANSFORM},
                {"name": PipelineStage.LOAD_DB},
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
        "raw_object_uri": source_object.get("object_uri") if source_object else None,
    }


def process_item(connection, item, cli_args):
    """선점한 KIS item 하나를 수집, 적재하고 item 상태를 SUCCESS 또는 SKIPPED로 완료한다."""
    asset_id = parse_asset_id(item.item_key)
    asset = load_asset(connection, asset_id)
    if asset is None:
        return complete_item(
            connection,
            item.item_id,
            "SKIPPED",
            checkpoint={"worker": "kis_price_worker", "item_key": item.item_key, "asset_id": asset_id},
            error_code="KIS_ASSET_NOT_FOUND",
            error_message="DB에 활성 asset 또는 현재 KIS provider symbol이 없어 주가 수집을 건너뜁니다.",
        )

    options = read_price_options(item.parameters or {}, cli_args)
    options = apply_incremental_window(connection, asset, options)
    if options["start_date"] > options["end_date"]:
        stats = Counter({
            "fetched_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "skipped_already_up_to_date": 1,
        })
        checkpoint = build_checkpoint(item, asset, options, stats, None)
        return complete_item(
            connection,
            item.item_id,
            "SKIPPED",
            checkpoint=checkpoint,
            error_code="KIS_ALREADY_UP_TO_DATE",
            error_message="DB의 최신 price_bar가 요청 종료일 이상이라 KIS API 호출을 생략했습니다.",
        )

    storage = build_storage(cli_args.output_dir, item)
    client = KISClient.from_env(min_request_interval=options["request_interval"])
    collected_at = datetime.now(timezone.utc)

    result = client.get_daily_prices(
        exchange_code=asset["provider_exchange_code"],
        symbol=asset["provider_symbol"],
        start_date=options["start_date"],
        end_date=options["end_date"],
        adjusted=options["adjusted"],
        max_pages=options["max_pages"],
    )
    source_object = write_raw_pages(storage, asset, options, result, collected_at)
    rows, statuses = build_price_rows(asset, result["bars"], options, collected_at)
    write_table_previews(storage, source_object, rows, statuses)

    stats = Counter()
    stats["fetched_rows"] = len(result["bars"])
    stats["valid_rows"] = len(rows)
    stats["invalid_rows"] = sum(1 for status in statuses if status["status"] == "INVALID")
    with connection.cursor() as cursor:
        source_object_id = upsert_source_object(cursor, source_object)
    stats["source_objects_upserted"] = 1
    stats["source_object_id"] = source_object_id

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
            lineage_count = upsert_price_bar_lineage(
                cursor,
                source_object_id,
                upserted_bars,
                collected_at,
            )

        stats["affected_rows"] = len(upserted_bars)
        stats["lineage_upserted"] = lineage_count
        checkpoint = build_checkpoint(item, asset, options, stats, source_object)
        return complete_item(connection, item.item_id, "SUCCESS", checkpoint=checkpoint)

    connection.rollback()
    checkpoint = build_checkpoint(item, asset, options, stats, source_object)
    return complete_item(
        connection,
        item.item_id,
        "SKIPPED",
        checkpoint=checkpoint,
        error_code="KIS_NO_VALID_PRICE_BARS",
        error_message="KIS가 반환한 일봉 중 price_bar에 적재 가능한 정상 OHLCV가 없습니다.",
    )


def print_claimed_item(item):
    """worker가 어떤 item을 선점했는지 CLI에서 바로 확인할 수 있게 출력한다."""
    print("claimed=true")
    print(json.dumps({
        "item_id": item.item_id,
        "job_id": item.job_id,
        "item_key": item.item_key,
        "parameters": item.parameters,
    }, ensure_ascii=False, indent=2, default=str))


def run_once(connection, args):
    """QUEUED 상태의 KIS item 하나를 선점해서 처리하고, 처리 여부를 반환한다."""
    item = claim_next_queued_item(
        connection,
        source="KIS",
        job_type=args.job_type.upper() if args.job_type else None,
    )
    if item is None:
        print("claimed=false")
        print("message=조건에 맞는 QUEUED KIS pipeline_job_item이 없습니다")
        return False

    print_claimed_item(item)
    try:
        next_job_status = process_item(connection, item, args)
        print("processed=true")
        print(f"next_job_status={next_job_status}")
    except Exception as error:
        LOGGER.exception("KIS item 처리 실패 item_id=%s", item.item_id)
        connection.rollback()
        next_job_status = fail_item(
            connection,
            item.item_id,
            "KIS_WORKER_FAILED",
            str(error),
            checkpoint={
                "worker": "kis_price_worker",
                "item_key": item.item_key,
            },
        )
        print("processed=false")
        print(f"next_job_status={next_job_status}")
        raise
    return True


def main():
    """KIS Job Item을 한 번 처리하거나, --loop 모드에서는 계속 polling하며 처리한다."""
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
