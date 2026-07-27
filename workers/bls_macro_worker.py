import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from backfills.bls_macro.backfill import apply_bls_macro_records, write_reports
from collectors.bls_client import BLSClient
from collectors.bls_collector import collect_bls
from collectors.common import LocalPipelineStorage
from pipeline.stages import PipelineMode, PipelineStage
from workers.db import connect_database
from workers.job_repository import claim_next_queued_item, complete_item, fail_item


LOGGER = logging.getLogger("bls-macro-worker")


def parse_args():
    """BLS worker 실행 범위와 출력 위치를 CLI 인자로 받는다."""
    parser = argparse.ArgumentParser(
        description="QUEUED BLS pipeline_job_item 하나를 선점해 BLS API 수집과 DB 적재를 수행합니다."
    )
    parser.add_argument(
        "--job-type",
        default="BLS_MACRO_BACKFILL",
        help="처리할 pipeline_job.job_type 필터입니다.",
    )
    parser.add_argument(
        "--output-dir",
        default="pipeline_output/bls_macro_worker",
        help="worker가 보존할 BLS raw 원본과 요약 리포트 경로입니다.",
    )
    parser.add_argument(
        "--include-calendar",
        action="store_true",
        help="item 처리 중 BLS 발표 캘린더도 함께 수집합니다. 기본값은 series timeseries만 수집합니다.",
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
        help="--loop 모드에서 새 item을 다시 확인하기 전 대기 시간입니다.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def parse_series_id(item_key):
    """BLS:{seriesId} item key에서 실제 BLS Series ID를 추출한다."""
    prefix = "BLS:"
    if not item_key.upper().startswith(prefix):
        raise ValueError(f"BLS item_key는 {prefix} 로 시작해야 합니다: {item_key}")
    series_id = item_key[len(prefix):].strip().upper()
    if not series_id:
        raise ValueError(f"BLS item_key에 series id가 없습니다: {item_key}")
    return series_id


def read_year_range(parameters):
    """pipeline_job.parameters에서 startYear/endYear를 읽고 기본값을 보정한다."""
    current_year = datetime.now(timezone.utc).year
    start_year = int(parameters.get("startYear") or parameters.get("start_year") or current_year - 2)
    end_year = int(parameters.get("endYear") or parameters.get("end_year") or current_year)
    if start_year > end_year:
        raise ValueError(f"startYear는 endYear보다 클 수 없습니다: {start_year}>{end_year}")
    return start_year, end_year


def build_storage(root, item):
    """Job과 item 단위로 raw/report 출력 경로가 겹치지 않도록 저장소를 만든다."""
    return LocalPipelineStorage(
        Path(root) / f"job-{item.job_id}" / f"item-{item.item_id}",
        pipeline_job_item_id=item.item_id,
    )


def load_series_metadata(connection, series_id):
    """DB의 macro_series 카탈로그에서 선택한 BLS 지표의 표시명과 단위 정보를 읽는다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT name, category, frequency, unit, seasonal_adjustment
            FROM macro_series
            WHERE provider = 'BLS'
              AND external_series_id = %s
            """,
            (series_id,),
        )
        row = cursor.fetchone()
    if row is None:
        raise LookupError(f"macro_series에 등록되지 않은 BLS series id입니다: {series_id}")
    return {
        series_id: {
            "name": row[0],
            "category": row[1],
            "frequency": row[2],
            "unit": row[3],
            "seasonal_adjustment": row[4],
        }
    }


def build_checkpoint(item, series_id, start_year, end_year, stats, summary_path, pipeline_run=None):
    """완료된 item의 재처리 기준과 결과 요약을 checkpoint JSON으로 구성한다."""
    pipeline_checkpoint = (
        pipeline_run.checkpoint()
        if pipeline_run
        else {
            "worker": "bls_macro_worker",
            "mode": PipelineMode.COMBINED,
            "stages": [
                {"name": PipelineStage.COLLECT_RAW},
                {"name": PipelineStage.WRITE_RAW_PREVIEW},
                {"name": PipelineStage.LOAD_DB},
            ],
        }
    )
    return {
        "worker": "bls_macro_worker",
        "pipeline": pipeline_checkpoint,
        "job_id": item.job_id,
        "item_id": item.item_id,
        "series_id": series_id,
        "start_year": start_year,
        "end_year": end_year,
        "stats": stats,
        "summary_report": str(summary_path),
    }


def process_item(connection, item, args):
    """선점한 BLS item 하나를 수집, 적재하고 item 상태를 SUCCESS 또는 SKIPPED로 완료한다."""
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
    for table_name, table_records in records.items():
        path, count = storage.write_table(table_name, table_records)
        LOGGER.info("%s rows=%d path=%s", table_name, count, path)

    stats = apply_bls_macro_records(connection, records)
    summary_path = write_reports(args.output_dir, stats, records)
    checkpoint = build_checkpoint(item, series_id, start_year, end_year, stats, summary_path)

    written_rows = (
        stats.get("macro_observations_upserted", 0)
        + stats.get("macro_observation_statuses_upserted", 0)
    )
    if written_rows == 0:
        return complete_item(
            connection,
            item.item_id,
            "SKIPPED",
            checkpoint=checkpoint,
            error_code="BLS_NO_OBSERVATIONS",
            error_message=f"{series_id} {start_year}-{end_year} 구간에 적재할 관측값이 없습니다.",
        )

    return complete_item(
        connection,
        item.item_id,
        "SUCCESS",
        checkpoint=checkpoint,
    )


def main():
    """BLS Job Item 하나를 실제 수집 worker 흐름으로 처리한다."""
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    connection = connect_database()
    try:
        item = claim_next_queued_item(
            connection,
            source="BLS",
            job_type=args.job_type.upper() if args.job_type else None,
        )
        if item is None:
            print("claimed=false")
            print("message=조건에 맞는 QUEUED BLS pipeline_job_item이 없습니다")
            return

        print("claimed=true")
        print(json.dumps({
            "item_id": item.item_id,
            "job_id": item.job_id,
            "item_key": item.item_key,
            "parameters": item.parameters,
        }, ensure_ascii=False, indent=2))

        try:
            next_job_status = process_item(connection, item, args)
            print("processed=true")
            print(f"next_job_status={next_job_status}")
        except Exception as error:
            LOGGER.exception("BLS item 처리 실패 item_id=%s", item.item_id)
            connection.rollback()
            next_job_status = fail_item(
                connection,
                item.item_id,
                "BLS_WORKER_FAILED",
                str(error),
                checkpoint={
                    "worker": "bls_macro_worker",
                    "item_key": item.item_key,
                },
            )
            print("processed=false")
            print(f"next_job_status={next_job_status}")
            raise
    finally:
        connection.close()


def run_once(connection, args):
    """QUEUED 상태의 BLS item 하나를 선점해서 처리하고, 처리 여부를 반환한다."""
    item = claim_next_queued_item(
        connection,
        source="BLS",
        job_type=args.job_type.upper() if args.job_type else None,
    )
    if item is None:
        print("claimed=false")
        print("message=조건에 맞는 QUEUED BLS pipeline_job_item이 없습니다")
        return False

    print("claimed=true")
    print(json.dumps({
        "item_id": item.item_id,
        "job_id": item.job_id,
        "item_key": item.item_key,
        "parameters": item.parameters,
    }, ensure_ascii=False, indent=2))

    try:
        next_job_status = process_item(connection, item, args)
        print("processed=true")
        print(f"next_job_status={next_job_status}")
    except Exception as error:
        LOGGER.exception("BLS item 처리 실패 item_id=%s", item.item_id)
        connection.rollback()
        next_job_status = fail_item(
            connection,
            item.item_id,
            "BLS_WORKER_FAILED",
            str(error),
            checkpoint={
                "worker": "bls_macro_worker",
                "item_key": item.item_key,
            },
        )
        print("processed=false")
        print(f"next_job_status={next_job_status}")
        raise
    return True


def loop_main():
    """BLS Job Item을 한 번 처리하거나, --loop 모드에서는 계속 polling하며 처리한다."""
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
    loop_main()
