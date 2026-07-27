import argparse
import json
import logging
import re
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from backfills.sec_financial.backfill import (
    apply_company_financials,
    collect_sec_financial_payloads,
    load_targets_by_cik,
    load_targets_by_ticker,
    normalize_cik,
    require_user_agent,
    upsert_source_object,
    write_reports,
)
from collectors.common import LocalPipelineStorage
from collectors.sec_client import SECClient
from pipeline.stages import PipelineMode, PipelineStage
from transformers.sec_transformer import transform_filings
from workers.db import connect_database
from workers.job_repository import claim_next_queued_item, complete_item, fail_item


LOGGER = logging.getLogger("sec-financial-worker")


def parse_args():
    """SEC financial worker 실행 범위와 polling 방식을 CLI 인자로 받는다."""
    parser = argparse.ArgumentParser(
        description="QUEUED SEC pipeline_job_item 하나를 선점해 SEC 재무 원본 수집과 DB 적재를 수행합니다."
    )
    parser.add_argument(
        "--job-type",
        default="SEC_FINANCIAL_BACKFILL",
        help="처리할 pipeline_job.job_type 필터입니다.",
    )
    parser.add_argument(
        "--output-dir",
        default="pipeline_output/sec_financial_worker",
        help="worker가 보존할 SEC raw 원본과 요약 리포트 경로입니다.",
    )
    parser.add_argument(
        "--include-history",
        action="store_true",
        help="Job parameters에 값이 없을 때 사용할 기본값입니다. SEC historical submissions도 함께 수집합니다.",
    )
    parser.add_argument(
        "--all-facts",
        action="store_true",
        help="Job parameters에 값이 없을 때 사용할 기본값입니다. 기본 core metric 외 모든 fact를 저장합니다.",
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=1.0,
        help="SEC 요청 사이 최소 대기 시간입니다. SEC rate limit을 피하기 위한 값입니다.",
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


def parse_sec_target(item_key):
    """SEC:{ticker|cik} item key를 ticker 또는 CIK 조회 조건으로 변환한다."""
    prefix = "SEC:"
    if not item_key.upper().startswith(prefix):
        raise ValueError(f"SEC item_key는 {prefix} 로 시작해야 합니다: {item_key}")

    raw_value = item_key[len(prefix):].strip()
    if not raw_value:
        raise ValueError(f"SEC item_key에 ticker 또는 CIK가 없습니다: {item_key}")

    if re.fullmatch(r"\d{1,10}", raw_value):
        return {"kind": "cik", "value": normalize_cik(raw_value)}
    return {"kind": "ticker", "value": raw_value.upper()}


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
    if value is None or value == "":
        return float(default)
    return float(value)


def read_int(parameters, key, default):
    """pipeline_job.parameters의 정수 옵션을 안전하게 읽는다."""
    value = parameters.get(key)
    if value is None or value == "":
        return int(default)
    return int(value)


def read_collection_mode(parameters):
    """SEC 수집 모드를 읽는다.

    BACKFILL은 기존처럼 대상 기업의 SEC 원본을 수집한다.
    INCREMENTAL은 company_collection_state와 submissions recent를 먼저 확인해
    아직 확인 주기가 아니거나 신규 accession이 없으면 companyfacts 호출을 생략한다.
    """
    value = parameters.get("mode") or parameters.get("collectionMode") or "BACKFILL"
    return str(value).strip().upper()


def build_backfill_args(item, cli_args):
    """기존 sec_financial.backfill 함수들이 기대하는 최소 args 객체를 만든다."""
    parameters = item.parameters or {}
    return SimpleNamespace(
        apply=True,
        include_history=read_bool(parameters, "includeHistory", cli_args.include_history),
        all_facts=read_bool(parameters, "allFacts", cli_args.all_facts),
        request_interval=read_float(parameters, "requestInterval", cli_args.request_interval),
        collection_mode=read_collection_mode(parameters),
        check_interval_hours=read_int(parameters, "checkIntervalHours", 168),
        force_check=read_bool(parameters, "forceCheck", False),
        force_company_facts=read_bool(parameters, "forceCompanyFacts", False),
        output_dir=str(Path(cli_args.output_dir) / f"job-{item.job_id}" / f"item-{item.item_id}"),
    )


def build_storage(output_dir, item=None):
    """Job item 단위로 raw/table/report 출력 경로가 겹치지 않도록 storage를 만든다."""
    return LocalPipelineStorage(
        Path(output_dir),
        pipeline_job_item_id=item.item_id if item else None,
    )


def load_collection_state(connection, company_id, provider="SEC", data_type="FINANCIAL"):
    """회사별 SEC 확인 상태를 조회한다.

    이 상태는 정규화된 재무 데이터가 아니라, 외부 API 호출 전에
    '지금 이 회사를 다시 확인해야 하는지' 판단하기 위한 운영 메타데이터다.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                last_checked_at,
                last_success_at,
                last_seen_external_id,
                next_check_at,
                check_interval_hours,
                priority,
                last_status,
                last_error_code,
                last_error_message
            FROM company_collection_state
            WHERE provider = %s
              AND company_id = %s
              AND data_type = %s
            """,
            (provider, company_id, data_type),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    return {
        "last_checked_at": row[0],
        "last_success_at": row[1],
        "last_seen_external_id": row[2],
        "next_check_at": row[3],
        "check_interval_hours": row[4],
        "priority": row[5],
        "last_status": row[6],
        "last_error_code": row[7],
        "last_error_message": row[8],
    }


def update_collection_state(
    connection,
    company_id,
    checked_at,
    check_interval_hours,
    status,
    last_seen_external_id=None,
    error_code=None,
    error_message=None,
    checkpoint=None,
    provider="SEC",
    data_type="FINANCIAL",
):
    """회사별 SEC 확인 상태를 upsert한다."""
    from psycopg2.extras import Json

    next_check_at = checked_at + timedelta(hours=check_interval_hours)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO company_collection_state (
                provider, company_id, data_type,
                last_checked_at, last_success_at, last_seen_external_id,
                next_check_at, check_interval_hours, last_status,
                last_error_code, last_error_message, checkpoint
            ) VALUES (
                %s, %s, %s,
                %s, CASE WHEN %s IN ('SUCCESS', 'SKIPPED', 'NO_CHANGE', 'NOT_DUE') THEN %s ELSE NULL END, %s,
                %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (provider, company_id, data_type)
            DO UPDATE SET
                last_checked_at = EXCLUDED.last_checked_at,
                last_success_at = CASE
                    WHEN EXCLUDED.last_success_at IS NOT NULL THEN EXCLUDED.last_success_at
                    ELSE company_collection_state.last_success_at
                END,
                last_seen_external_id = COALESCE(
                    EXCLUDED.last_seen_external_id,
                    company_collection_state.last_seen_external_id
                ),
                next_check_at = EXCLUDED.next_check_at,
                check_interval_hours = EXCLUDED.check_interval_hours,
                last_status = EXCLUDED.last_status,
                last_error_code = EXCLUDED.last_error_code,
                last_error_message = EXCLUDED.last_error_message,
                checkpoint = EXCLUDED.checkpoint,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                provider,
                company_id,
                data_type,
                checked_at,
                status,
                checked_at,
                last_seen_external_id,
                next_check_at,
                check_interval_hours,
                status,
                error_code,
                error_message,
                Json(checkpoint) if checkpoint is not None else None,
            ),
        )


def is_incremental_mode(backfill_args):
    """SEC worker가 최신 확인 최적화를 사용할 모드인지 판단한다."""
    return backfill_args.collection_mode in {
        "INCREMENTAL",
        "SUBMISSIONS_FIRST",
        "LATEST_ONLY",
    }


def state_is_not_due(state, now):
    """next_check_at이 미래이면 외부 API 호출을 생략해도 되는 상태로 본다."""
    return state is not None and state.get("next_check_at") and state["next_check_at"] > now


def latest_accession(filing_records):
    """recent submissions에서 가장 최신 accession number를 고른다."""
    if not filing_records:
        return None
    ordered = sorted(
        filing_records,
        key=lambda record: (
            record.get("accepted_at") or "",
            record.get("filing_date") or "",
            record.get("accession_number") or "",
        ),
        reverse=True,
    )
    return ordered[0].get("accession_number")


def collect_submissions_recent_only(client, storage, target, collected_at):
    """SEC submissions recent만 먼저 수집해 신규 filing 여부를 빠르게 판단한다."""
    cik = target["cik"]
    submissions = client.get_submissions(cik)
    submissions_source = storage.write_raw_json(
        "SEC",
        "SUBMISSIONS",
        cik,
        submissions,
        collected_at,
    )
    filing_records = transform_filings(
        submissions,
        [submissions.get("filings", {}).get("recent", {})],
        submissions_source["object_uri"],
        collected_at,
    )
    filing_records = list(
        {record["accession_number"]: record for record in filing_records}.values()
    )
    return {
        "source_object": [submissions_source],
        "filing": filing_records,
        "latest_accession": latest_accession(filing_records),
    }


def load_single_target(connection, item_key):
    """item_key 기준으로 DB에 이미 연결된 company/asset 대상을 하나 찾는다."""
    target_ref = parse_sec_target(item_key)
    if target_ref["kind"] == "cik":
        targets = load_targets_by_cik(connection, [target_ref["value"]])
    else:
        targets = load_targets_by_ticker(connection, [target_ref["value"]])

    if not targets:
        return None, target_ref
    return targets[0], target_ref


def write_table_previews(storage, records):
    """수집/변환 결과를 DB 적재 전후에 사람이 볼 수 있도록 JSONL로 남긴다."""
    for table_name in ("source_object", "filing", "financial_fact"):
        path, count = storage.write_table(table_name, records.get(table_name, []))
        LOGGER.info("%s rows=%d path=%s", table_name, count, path)


def build_checkpoint(item, target, backfill_args, stats, summary_path, skipped_path, failed_path):
    """완료된 item의 재처리 기준과 결과 요약을 checkpoint JSON으로 구성한다."""
    return {
        "worker": "sec_financial_worker",
        "pipeline": {
            "worker": "sec_financial_worker",
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
        "ticker": target.get("ticker"),
        "cik": target.get("cik"),
        "company_id": target.get("company_id"),
        "include_history": backfill_args.include_history,
        "all_facts": backfill_args.all_facts,
        "request_interval": backfill_args.request_interval,
        "stats": dict(sorted(stats.items())),
        "summary_report": str(summary_path),
        "skipped_companies_report": str(skipped_path) if skipped_path else None,
        "failed_report": str(failed_path) if failed_path else None,
    }


def process_item(connection, item, cli_args):
    """선점한 SEC item 하나를 수집, 적재하고 item 상태를 SUCCESS 또는 SKIPPED로 완료한다."""
    target, target_ref = load_single_target(connection, item.item_key)
    if target is None:
        return complete_item(
            connection,
            item.item_id,
            "SKIPPED",
            checkpoint={
                "worker": "sec_financial_worker",
                "item_key": item.item_key,
                "target": target_ref,
            },
            error_code="SEC_TARGET_NOT_FOUND",
            error_message="DB에 연결된 company/asset 대상이 없어 SEC 재무 수집을 건너뜁니다.",
        )

    backfill_args = build_backfill_args(item, cli_args)
    storage = build_storage(backfill_args.output_dir, item)
    client = SECClient(
        require_user_agent(),
        min_request_interval=backfill_args.request_interval,
    )

    collected_at = datetime.now(timezone.utc)
    collection_state = load_collection_state(connection, target["company_id"])
    if (
        is_incremental_mode(backfill_args)
        and not backfill_args.force_check
        and state_is_not_due(collection_state, collected_at)
    ):
        checkpoint = {
            "worker": "sec_financial_worker",
            "job_id": item.job_id,
            "item_id": item.item_id,
            "item_key": item.item_key,
            "ticker": target.get("ticker"),
            "cik": target.get("cik"),
            "company_id": target.get("company_id"),
            "collection_mode": backfill_args.collection_mode,
            "next_check_at": collection_state["next_check_at"].isoformat(),
            "last_seen_accession_number": collection_state.get("last_seen_external_id"),
        }
        return complete_item(
            connection,
            item.item_id,
            "SKIPPED",
            checkpoint=checkpoint,
            error_code="SEC_CHECK_NOT_DUE",
            error_message="company_collection_state.next_check_at이 아직 지나지 않아 SEC API 호출을 생략했습니다.",
        )

    stats = Counter()
    skipped_facts = []
    skipped_companies = []
    failed_companies = []
    processed_companies = []

    if is_incremental_mode(backfill_args):
        submissions_snapshot = collect_submissions_recent_only(
            client,
            storage,
            target,
            collected_at,
        )
        latest_seen_accession = submissions_snapshot["latest_accession"]
        previous_accession = (
            collection_state.get("last_seen_external_id")
            if collection_state
            else None
        )
        if (
            latest_seen_accession
            and previous_accession == latest_seen_accession
            and not backfill_args.force_company_facts
        ):
            with connection.cursor() as cursor:
                for source in submissions_snapshot["source_object"]:
                    upsert_source_object(cursor, source)
            stats["source_objects"] += len(submissions_snapshot["source_object"])
            stats["source_objects_upserted"] += len(submissions_snapshot["source_object"])
            stats["filings_seen"] += len(submissions_snapshot["filing"])
            stats["skipped_no_new_filings"] += 1
            checkpoint = {
                "worker": "sec_financial_worker",
                "job_id": item.job_id,
                "item_id": item.item_id,
                "item_key": item.item_key,
                "ticker": target.get("ticker"),
                "cik": target.get("cik"),
                "company_id": target.get("company_id"),
                "collection_mode": backfill_args.collection_mode,
                "latest_accession_number": latest_seen_accession,
                "previous_accession_number": previous_accession,
                "stats": dict(sorted(stats.items())),
            }
            update_collection_state(
                connection,
                target["company_id"],
                collected_at,
                backfill_args.check_interval_hours,
                "NO_CHANGE",
                last_seen_external_id=latest_seen_accession,
                checkpoint=checkpoint,
            )
            connection.commit()
            return complete_item(
                connection,
                item.item_id,
                "SKIPPED",
                checkpoint=checkpoint,
                error_code="SEC_NO_NEW_FILINGS",
                error_message="SEC submissions recent의 최신 accession이 이전 확인값과 같아 companyfacts 호출을 생략했습니다.",
            )

    records = collect_sec_financial_payloads(
        client,
        storage,
        target,
        backfill_args,
        collected_at,
    )
    write_table_previews(storage, records)

    stats["source_objects"] += len(records["source_object"])
    stats["filings_seen"] += len(records["filing"])
    stats["facts_seen"] += len(records["financial_fact"])
    latest_seen_accession = latest_accession(records["filing"])

    status = "SUCCESS"
    error_code = None
    error_message = None
    if records.get("company_facts_missing"):
        stats["skipped_no_company_facts"] += 1
        status = "SKIPPED"
        error_code = "SEC_NO_COMPANY_FACTS"
        error_message = "SEC companyfacts endpoint가 없어 재무 fact 적재를 건너뜁니다."
        skipped_companies.append({"ticker": target.get("ticker"), "cik": target["cik"], "reason": error_code})
    elif not records["financial_fact"]:
        stats["skipped_no_core_facts"] += 1
        status = "SKIPPED"
        error_code = "SEC_NO_CORE_FACTS"
        error_message = "수집된 SEC companyfacts 중 현재 매핑된 core financial fact가 없습니다."
        skipped_companies.append({"ticker": target.get("ticker"), "cik": target["cik"], "reason": error_code})
    else:
        apply_company_financials(
            connection,
            target,
            records,
            collected_at,
            stats,
            skipped_facts,
        )

    processed_companies.append(
        {
            "ticker": target.get("ticker"),
            "cik": target["cik"],
            "company_id": target.get("company_id"),
            "status": status,
            "source_objects": len(records["source_object"]),
            "filings_seen": len(records["filing"]),
            "facts_seen": len(records["financial_fact"]),
            "facts_upserted": stats["facts_upserted"],
        }
    )
    summary_path, failed_path, skipped_path, skipped_companies_path, _ = write_reports(
        backfill_args.output_dir,
        stats,
        failed_companies,
        skipped_facts,
        skipped_companies,
        processed_companies,
    )
    checkpoint = build_checkpoint(
        item,
        target,
        backfill_args,
        stats,
        summary_path,
        skipped_companies_path or skipped_path,
        failed_path,
    )
    checkpoint["collection_mode"] = backfill_args.collection_mode
    checkpoint["latest_accession_number"] = latest_seen_accession

    update_collection_state(
        connection,
        target["company_id"],
        collected_at,
        backfill_args.check_interval_hours,
        status,
        last_seen_external_id=latest_seen_accession,
        error_code=error_code,
        error_message=error_message,
        checkpoint=checkpoint,
    )

    return complete_item(
        connection,
        item.item_id,
        status,
        checkpoint=checkpoint,
        error_code=error_code,
        error_message=error_message,
    )


def print_claimed_item(item):
    """worker가 어떤 item을 선점했는지 CLI에서 바로 확인할 수 있게 출력한다."""
    print("claimed=true")
    print(json.dumps({
        "item_id": item.item_id,
        "job_id": item.job_id,
        "item_key": item.item_key,
        "parameters": item.parameters,
    }, ensure_ascii=False, indent=2))


def run_once(connection, args):
    """QUEUED 상태의 SEC item 하나를 선점해서 처리하고, 처리 여부를 반환한다."""
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
        LOGGER.exception("SEC item 처리 실패 item_id=%s", item.item_id)
        connection.rollback()
        next_job_status = fail_item(
            connection,
            item.item_id,
            "SEC_WORKER_FAILED",
            str(error),
            checkpoint={
                "worker": "sec_financial_worker",
                "item_key": item.item_key,
            },
        )
        print("processed=false")
        print(f"next_job_status={next_job_status}")
        raise
    return True


def main():
    """SEC Job Item을 한 번 처리하거나, --loop 모드에서는 계속 polling하며 처리한다."""
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
