import argparse
import json
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv():
        """python-dotenv가 없는 환경에서는 이미 설정된 환경 변수만 사용한다."""
        return False

from collectors.common import (
    LocalPipelineStorage,
    build_target_record_key,
    transformed_record_hash,
)
from collectors.sec_client import SECClient
from transformers.sec_transformer import (
    TRANSFORMER_NAME,
    TRANSFORMER_VERSION,
    transform_filings,
    transform_financial_facts,
)


LOGGER = logging.getLogger("sec-financial-backfill")
BACKFILL_TRANSFORMER_NAME = "sec-financial-backfill"
BACKFILL_TRANSFORMER_VERSION = "1.0.0"


def is_http_status(error, status_code):
    """requests HTTPError 계열 오류에서 특정 HTTP 상태인지 안전하게 확인한다."""
    response = getattr(error, "response", None)
    return response is not None and response.status_code == status_code


DEFAULT_COMPANIES = {
    "AAPL": "0000320193",
    "NVDA": "0001045810",
}

FINANCIAL_METRICS = {
    "REVENUE": {
        "name": "Revenue",
        "statement_type": "INCOME_STATEMENT",
        "value_type": "MONETARY",
        "default_unit": "USD",
    },
    "NET_INCOME": {
        "name": "Net Income",
        "statement_type": "INCOME_STATEMENT",
        "value_type": "MONETARY",
        "default_unit": "USD",
    },
    "OPERATING_INCOME": {
        "name": "Operating Income",
        "statement_type": "INCOME_STATEMENT",
        "value_type": "MONETARY",
        "default_unit": "USD",
    },
    "ASSETS": {
        "name": "Assets",
        "statement_type": "BALANCE_SHEET",
        "value_type": "MONETARY",
        "default_unit": "USD",
    },
    "LIABILITIES": {
        "name": "Liabilities",
        "statement_type": "BALANCE_SHEET",
        "value_type": "MONETARY",
        "default_unit": "USD",
    },
    "STOCKHOLDERS_EQUITY": {
        "name": "Stockholders Equity",
        "statement_type": "BALANCE_SHEET",
        "value_type": "MONETARY",
        "default_unit": "USD",
    },
    "DILUTED_EPS": {
        "name": "Diluted EPS",
        "statement_type": "INCOME_STATEMENT",
        "value_type": "PER_SHARE",
        "default_unit": "USD/shares",
    },
    "OPERATING_CASH_FLOW": {
        "name": "Operating Cash Flow",
        "statement_type": "CASH_FLOW",
        "value_type": "MONETARY",
        "default_unit": "USD",
    },
    "CASH": {
        "name": "Cash and Cash Equivalents",
        "statement_type": "BALANCE_SHEET",
        "value_type": "MONETARY",
        "default_unit": "USD",
    },
    "CAPEX": {
        "name": "Capital Expenditure",
        "statement_type": "CASH_FLOW",
        "value_type": "MONETARY",
        "default_unit": "USD",
    },
}


def parse_args():
    """SEC 재무 백필 범위와 DB 반영 여부를 명령행 인자로 받는다."""
    parser = argparse.ArgumentParser(
        description="Backfill SEC filings and company facts into financial tables."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes to PostgreSQL. Without this flag, only fetch and validate.",
    )
    parser.add_argument(
        "--ticker",
        action="append",
        help="Process one current asset ticker from DB. Repeat for multiple tickers.",
    )
    parser.add_argument(
        "--company",
        action="append",
        type=parse_company_arg,
        help="Process an explicit company as TICKER=CIK. Useful for dry-run samples.",
    )
    parser.add_argument(
        "--cik",
        action="append",
        help="Process one CIK already loaded in company. Repeat for multiple CIKs.",
    )
    parser.add_argument(
        "--matched-companies",
        action="store_true",
        help="Process companies linked to at least one active asset.",
    )
    parser.add_argument("--limit", type=int)
    parser.add_argument("--start-after-cik", default="0000000000")
    parser.add_argument("--request-interval", type=float, default=0.2)
    parser.add_argument(
        "--auto-batches",
        action="store_true",
        help="Repeat matched company backfill in cursor-based batches until empty.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Number of companies per auto batch. Defaults to --limit or 100.",
    )
    parser.add_argument(
        "--batch-sleep-seconds",
        type=float,
        default=5.0,
        help="Pause between auto batches to avoid hammering SEC.",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        help="Safety cap for auto batch count.",
    )
    parser.add_argument(
        "--include-history",
        action="store_true",
        help="Fetch historical submissions pages listed by SEC submissions.files.",
    )
    parser.add_argument(
        "--all-facts",
        action="store_true",
        help="Store every SEC fact. Default stores mapped core metrics only.",
    )
    parser.add_argument(
        "--output-dir",
        default="pipeline_output/sec_financial_backfill",
        help="Raw JSON and report output directory.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def parse_company_arg(value):
    """TICKER=CIK 입력을 백필 대상 딕셔너리로 변환한다."""
    if "=" not in value:
        raise argparse.ArgumentTypeError("Use TICKER=CIK, for example AAPL=0000320193")
    ticker, cik = value.split("=", 1)
    ticker = ticker.strip().upper()
    cik = normalize_cik(cik)
    if not ticker:
        raise argparse.ArgumentTypeError(f"Invalid company value: {value}")
    return {"ticker": ticker, "cik": cik, "company_id": None}


def normalize_cik(value):
    """숫자 또는 문자열 CIK를 SEC API가 요구하는 10자리 문자열로 맞춘다."""
    digits = re.sub(r"\D", "", str(value or ""))
    if not digits or len(digits) > 10:
        raise ValueError(f"invalid CIK: {value!r}")
    return digits.zfill(10)


def require_user_agent():
    """SEC 정책상 필요한 연락처 포함 User-Agent를 환경 변수에서 읽는다."""
    value = os.getenv("SEC_USER_AGENT")
    if not value:
        raise RuntimeError("SEC_USER_AGENT is required in .env or the environment")
    return value


def require_database_url():
    """PostgreSQL 접속 문자열을 환경 변수에서 읽고 없으면 안전하게 중단한다."""
    value = os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError(
            "DATABASE_URL is required, for example "
            "postgresql://postgres:<password>@localhost:55432/trader"
        )
    return value


def connect_database_if_needed(args):
    """DB가 필요한 옵션일 때만 PostgreSQL 연결을 생성한다."""
    needs_db = args.apply or args.ticker or args.cik or args.matched_companies
    if not needs_db:
        return None
    import psycopg2

    connection = psycopg2.connect(require_database_url())
    connection.autocommit = False
    return connection


def load_target_companies(connection, args):
    """CLI 옵션에 따라 백필할 company 목록을 만든다."""
    explicit = list(args.company or [])
    if not any([args.ticker, args.cik, args.matched_companies]) and not explicit:
        explicit = [
            {"ticker": ticker, "cik": cik, "company_id": None}
            for ticker, cik in DEFAULT_COMPANIES.items()
        ]

    targets = []
    targets.extend(explicit)
    if args.ticker:
        if connection is None:
            raise RuntimeError("--ticker requires DATABASE_URL")
        targets.extend(load_targets_by_ticker(connection, args.ticker))
    if args.cik:
        if connection is None:
            raise RuntimeError("--cik requires DATABASE_URL")
        targets.extend(load_targets_by_cik(connection, args.cik))
    if args.matched_companies:
        if connection is None:
            raise RuntimeError("--matched-companies requires DATABASE_URL")
        targets.extend(load_matched_company_targets(connection, args))

    start_after = normalize_cik(args.start_after_cik)
    unique = {}
    for target in targets:
        cik = normalize_cik(target["cik"])
        if cik <= start_after:
            continue
        unique[cik] = {
            "ticker": target.get("ticker") or cik,
            "cik": cik,
            "company_id": target.get("company_id"),
        }

    selected = [unique[cik] for cik in sorted(unique)]
    if args.limit is not None:
        selected = selected[: args.limit]
    return selected


def load_targets_by_ticker(connection, tickers):
    """현재 active asset의 ticker를 기준으로 연결된 company를 조회한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT a.ticker, c.cik, c.id
            FROM asset a
            JOIN company c ON c.id = a.company_id
            WHERE a.active = TRUE
              AND a.ticker = ANY(%s)
            ORDER BY c.cik
            """,
            ([ticker.strip().upper() for ticker in tickers],),
        )
        return [
            {"ticker": ticker, "cik": cik, "company_id": company_id}
            for ticker, cik, company_id in cursor.fetchall()
        ]


def load_targets_by_cik(connection, ciks):
    """DB에 이미 적재된 company CIK를 기준으로 백필 대상을 조회한다."""
    normalized_ciks = [normalize_cik(cik) for cik in ciks]
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT c.cik, c.id, COALESCE(MIN(a.ticker), c.cik) AS ticker
            FROM company c
            LEFT JOIN asset a ON a.company_id = c.id AND a.active = TRUE
            WHERE c.cik = ANY(%s)
            GROUP BY c.cik, c.id
            ORDER BY c.cik
            """,
            (normalized_ciks,),
        )
        return [
            {"ticker": ticker, "cik": cik, "company_id": company_id}
            for cik, company_id, ticker in cursor.fetchall()
        ]


def load_matched_company_targets(connection, args):
    """SEC company와 연결된 active asset이 있는 기업 목록을 조회한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT c.cik, c.id, MIN(a.ticker) AS ticker
            FROM company c
            JOIN asset a ON a.company_id = c.id
            WHERE a.active = TRUE
              AND c.cik > %s
            GROUP BY c.cik, c.id
            ORDER BY c.cik
            LIMIT COALESCE(%s, 2147483647)
            """,
            (normalize_cik(args.start_after_cik), args.limit),
        )
        return [
            {"ticker": ticker, "cik": cik, "company_id": company_id}
            for cik, company_id, ticker in cursor.fetchall()
        ]


def fetch_existing_company_id(connection, cik):
    """명시 CIK가 DB에 존재할 때 company.id를 찾아 FK 연결에 사용한다."""
    if connection is None:
        return None
    with connection.cursor() as cursor:
        cursor.execute("SELECT id FROM company WHERE cik = %s", (cik,))
        row = cursor.fetchone()
        return row[0] if row else None


def collect_sec_financial_payloads(client, storage, target, args, collected_at):
    """SEC submissions와 companyfacts 원본을 수집하고 스키마형 레코드로 변환한다."""
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
    source_objects = [submissions_source]

    if args.include_history:
        for historical_file in submissions.get("filings", {}).get("files", []):
            file_name = historical_file.get("name")
            if not file_name:
                continue
            page = client.get_historical_submission_page(file_name)
            page_source = storage.write_raw_json(
                "SEC",
                "SUBMISSIONS_HISTORY",
                file_name,
                page,
                collected_at,
            )
            source_objects.append(page_source)
            filing_records.extend(
                transform_filings(
                    submissions,
                    [page],
                    page_source["object_uri"],
                    collected_at,
                )
            )

    company_facts_missing = False
    try:
        company_facts = client.get_company_facts(cik)
    except Exception as error:
        if not is_http_status(error, 404):
            raise
        LOGGER.warning("CIK=%s has no SEC companyfacts endpoint; skipping facts", cik)
        company_facts = None
        facts_source = None
        company_facts_missing = True
    else:
        facts_source = storage.write_raw_json(
            "SEC",
            "COMPANY_FACTS",
            cik,
            company_facts,
            collected_at,
        )
        source_objects.append(facts_source)

    filing_records = list(
        {record["accession_number"]: record for record in filing_records}.values()
    )
    financial_fact_records = []
    if company_facts is not None:
        financial_fact_records = transform_financial_facts(
            company_facts,
            facts_source["object_uri"],
            collected_at,
            core_metrics_only=not args.all_facts,
        )
    return {
        "source_object": source_objects,
        "filing": filing_records,
        "financial_fact": financial_fact_records,
        "company_facts_missing": company_facts_missing,
    }


def seed_financial_metrics(cursor):
    """서비스 표준 재무 지표 사전을 financial_metric에 멱등 저장한다."""
    metric_id_by_code = {}
    for metric_code, metric in FINANCIAL_METRICS.items():
        cursor.execute(
            """
            INSERT INTO financial_metric (
                metric_code, name, statement_type, value_type, default_unit
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (metric_code)
            DO UPDATE SET
                name = EXCLUDED.name,
                statement_type = EXCLUDED.statement_type,
                value_type = EXCLUDED.value_type,
                default_unit = EXCLUDED.default_unit
            RETURNING id
            """,
            (
                metric_code,
                metric["name"],
                metric["statement_type"],
                metric["value_type"],
                metric["default_unit"],
            ),
        )
        metric_id_by_code[metric_code] = cursor.fetchone()[0]
    return metric_id_by_code


def upsert_source_object(cursor, source):
    """로컬에 보존한 SEC 원본 메타데이터를 source_object에 멱등 저장한다."""
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


def upsert_filings(cursor, company_id, filing_records):
    """SEC 공시 메타데이터를 저장하고 accession_number에서 filing.id로 가는 맵을 만든다."""
    filing_id_by_accession = {}
    for filing in filing_records:
        cursor.execute(
            """
            INSERT INTO filing (
                company_id, accession_number, form_type, filing_date, report_date,
                accepted_at, primary_document, document_url, is_xbrl,
                is_inline_xbrl, amendment, raw_object_uri, collected_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (accession_number)
            DO UPDATE SET
                company_id = EXCLUDED.company_id,
                form_type = EXCLUDED.form_type,
                filing_date = EXCLUDED.filing_date,
                report_date = EXCLUDED.report_date,
                accepted_at = EXCLUDED.accepted_at,
                primary_document = EXCLUDED.primary_document,
                document_url = EXCLUDED.document_url,
                is_xbrl = EXCLUDED.is_xbrl,
                is_inline_xbrl = EXCLUDED.is_inline_xbrl,
                amendment = EXCLUDED.amendment,
                raw_object_uri = EXCLUDED.raw_object_uri,
                collected_at = EXCLUDED.collected_at
            RETURNING id
            """,
            (
                company_id,
                filing["accession_number"],
                filing["form_type"],
                filing["filing_date"],
                filing["report_date"],
                filing["accepted_at"],
                filing["primary_document"],
                filing["document_url"],
                to_bool_or_none(filing["is_xbrl"]),
                to_bool_or_none(filing["is_inline_xbrl"]),
                filing["amendment"],
                filing["raw_object_uri"],
                filing["collected_at"],
            ),
        )
        filing_id_by_accession[filing["accession_number"]] = cursor.fetchone()[0]
    return filing_id_by_accession


def to_bool_or_none(value):
    """SEC가 0/1, true/false, 문자열로 주는 boolean 값을 DB boolean 타입으로 맞춘다."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    return None


def upsert_financial_facts(
    cursor,
    company_id,
    financial_fact_records,
    filing_id_by_accession,
    metric_id_by_code,
    stats,
    skipped_facts,
):
    """SEC XBRL fact를 filing과 metric에 연결해 financial_fact에 저장한다."""
    from psycopg2.extras import Json

    fact_id_by_hash = {}
    for fact in financial_fact_records:
        filing_id = filing_id_by_accession.get(fact["accession_number"])
        if filing_id is None:
            stats["skipped_missing_filing"] += 1
            skipped_facts.append(
                skipped_fact_record(fact, "MISSING_FILING")
            )
            continue

        metric_code = fact.get("metric_code")
        metric_id = metric_id_by_code.get(metric_code) if metric_code else None
        if metric_code and metric_id is None:
            stats["skipped_missing_metric"] += 1
            skipped_facts.append(
                skipped_fact_record(fact, "MISSING_METRIC")
            )
            continue

        cursor.execute(
            """
            INSERT INTO financial_fact (
                company_id, filing_id, metric_id, taxonomy, source_tag,
                unit, period_start, period_end, value, fiscal_year,
                fiscal_period, form_type, frame, dimensions,
                source_fact_hash, collected_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s
            )
            ON CONFLICT (source_fact_hash)
            DO UPDATE SET
                company_id = EXCLUDED.company_id,
                filing_id = EXCLUDED.filing_id,
                metric_id = EXCLUDED.metric_id,
                taxonomy = EXCLUDED.taxonomy,
                source_tag = EXCLUDED.source_tag,
                unit = EXCLUDED.unit,
                period_start = EXCLUDED.period_start,
                period_end = EXCLUDED.period_end,
                value = EXCLUDED.value,
                fiscal_year = EXCLUDED.fiscal_year,
                fiscal_period = EXCLUDED.fiscal_period,
                form_type = EXCLUDED.form_type,
                frame = EXCLUDED.frame,
                dimensions = EXCLUDED.dimensions,
                collected_at = EXCLUDED.collected_at
            RETURNING id
            """,
            (
                company_id,
                filing_id,
                metric_id,
                fact["taxonomy"],
                fact["source_tag"],
                fact["unit"],
                fact["period_start"],
                fact["period_end"],
                fact["value"],
                fact["fiscal_year"],
                fact["fiscal_period"],
                fact["form_type"],
                fact["frame"],
                Json(fact["dimensions"]) if fact.get("dimensions") else None,
                fact["source_fact_hash"],
                fact["collected_at"],
            ),
        )
        fact_id_by_hash[fact["source_fact_hash"]] = cursor.fetchone()[0]
        stats["facts_upserted"] += 1
    return fact_id_by_hash


def skipped_fact_record(fact, reason):
    """저장하지 못한 fact를 나중에 분석하기 쉬운 리포트 레코드로 만든다."""
    return {
        "reason": reason,
        "company_cik": fact.get("company_cik"),
        "accession_number": fact.get("accession_number"),
        "metric_code": fact.get("metric_code"),
        "source_tag": fact.get("source_tag"),
        "period_end": fact.get("period_end"),
        "source_fact_hash": fact.get("source_fact_hash"),
    }


def upsert_lineage(
    cursor,
    source_object_id,
    target_table,
    target_record_id,
    target_record_key,
    transformed_record,
    processed_at,
):
    """source_object와 정규화 결과 레코드를 record_lineage로 연결한다."""
    cursor.execute(
        """
        INSERT INTO record_lineage (
            source_object_id, target_table, target_record_id, target_record_key,
            transformer_name, transformer_version, output_hash, processed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (
            source_object_id, target_table, target_record_key,
            transformer_name, transformer_version
        ) DO UPDATE SET
            target_record_id = EXCLUDED.target_record_id,
            output_hash = EXCLUDED.output_hash,
            processed_at = EXCLUDED.processed_at
        """,
        (
            source_object_id,
            target_table,
            target_record_id,
            target_record_key,
            BACKFILL_TRANSFORMER_NAME,
            BACKFILL_TRANSFORMER_VERSION,
            transformed_record_hash(transformed_record),
            processed_at,
        ),
    )


def apply_company_financials(connection, target, records, processed_at, stats, skipped_facts):
    """기업 하나의 SEC source, filing, financial_fact, lineage를 한 트랜잭션에 저장한다."""
    with connection.cursor() as cursor:
        metric_id_by_code = seed_financial_metrics(cursor)
        source_id_by_uri = {
            source["object_uri"]: upsert_source_object(cursor, source)
            for source in records["source_object"]
        }
        filing_id_by_accession = upsert_filings(
            cursor,
            target["company_id"],
            records["filing"],
        )
        fact_id_by_hash = upsert_financial_facts(
            cursor,
            target["company_id"],
            records["financial_fact"],
            filing_id_by_accession,
            metric_id_by_code,
            stats,
            skipped_facts,
        )
        for filing in records["filing"]:
            source_object_id = source_id_by_uri.get(filing["raw_object_uri"])
            filing_id = filing_id_by_accession.get(filing["accession_number"])
            if source_object_id and filing_id:
                upsert_lineage(
                    cursor,
                    source_object_id,
                    "filing",
                    filing_id,
                    build_target_record_key(
                        "filing",
                        {"accession_number": filing["accession_number"]},
                    ),
                    filing,
                    processed_at,
                )
        for fact in records["financial_fact"]:
            source_object_id = source_id_by_uri.get(fact["raw_object_uri"])
            fact_id = fact_id_by_hash.get(fact["source_fact_hash"])
            if source_object_id and fact_id:
                upsert_lineage(
                    cursor,
                    source_object_id,
                    "financial_fact",
                    fact_id,
                    build_target_record_key(
                        "financial_fact",
                        {"source_fact_hash": fact["source_fact_hash"]},
                    ),
                    fact,
                    processed_at,
                )
    connection.commit()


def collect_dry_run_stats(records, stats):
    """DB 반영 없이 수집/변환 결과의 크기만 집계한다."""
    stats["source_objects"] += len(records["source_object"])
    stats["filings_seen"] += len(records["filing"])
    stats["facts_seen"] += len(records["financial_fact"])
    stats["facts_core"] += sum(
        1 for fact in records["financial_fact"] if fact.get("metric_code")
    )


def write_jsonl(path, records):
    """실패/스킵 리포트를 JSONL 파일로 저장한다."""
    records = list(records)
    if not records:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as output:
        for record in records:
            output.write(json.dumps(record, ensure_ascii=False, default=str))
            output.write("\n")
    return path


def write_reports(
    output_dir,
    stats,
    failed_companies,
    skipped_facts,
    skipped_companies,
    processed_companies,
):
    """백필 실행 결과를 사람이 확인할 수 있는 summary와 상세 리포트로 저장한다."""
    report_dir = Path(output_dir) / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    summary_path = report_dir / f"summary_{stamp}.json"
    summary = dict(sorted(stats.items()))
    if processed_companies:
        summary["attempted_companies"] = len(processed_companies)
        summary["first_attempted_cik"] = processed_companies[0]["cik"]
        summary["last_attempted_cik"] = processed_companies[-1]["cik"]
        summary["next_start_after_cik"] = processed_companies[-1]["cik"]
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    failed_path = write_jsonl(
        report_dir / f"failed_companies_{stamp}.jsonl",
        failed_companies,
    )
    skipped_path = write_jsonl(
        report_dir / f"skipped_facts_{stamp}.jsonl",
        skipped_facts,
    )
    skipped_companies_path = write_jsonl(
        report_dir / f"skipped_companies_{stamp}.jsonl",
        skipped_companies,
    )
    processed_path = write_jsonl(
        report_dir / f"processed_companies_{stamp}.jsonl",
        processed_companies,
    )
    return summary_path, failed_path, skipped_path, skipped_companies_path, processed_path


def run_backfill(client, storage, connection, targets, args):
    """대상 기업을 순회하며 SEC 재무 백필을 수행하고 기업 단위 실패를 격리한다."""
    stats = Counter()
    failed_companies = []
    skipped_facts = []
    skipped_companies = []
    processed_companies = []

    for index, target in enumerate(targets, start=1):
        cik = target["cik"]
        started_at = datetime.now(timezone.utc)
        try:
            if target.get("company_id") is None:
                target["company_id"] = fetch_existing_company_id(connection, cik)
            if args.apply and target.get("company_id") is None:
                raise ValueError(f"company CIK={cik} is not loaded in DB")

            collected_at = datetime.now(timezone.utc)
            records = collect_sec_financial_payloads(
                client,
                storage,
                target,
                args,
                collected_at,
            )
            collect_dry_run_stats(records, stats)
            status = "PROCESSED"
            skip_reason = None
            if records.get("company_facts_missing"):
                stats["skipped_no_company_facts"] += 1
                status = "SKIPPED"
                skip_reason = "NO_COMPANY_FACTS"
                skipped_companies.append(
                    {
                        "ticker": target.get("ticker"),
                        "cik": cik,
                        "reason": skip_reason,
                    }
                )
            elif not records["financial_fact"]:
                stats["skipped_no_core_facts"] += 1
                status = "SKIPPED"
                skip_reason = "NO_CORE_FACTS"
                skipped_companies.append(
                    {
                        "ticker": target.get("ticker"),
                        "cik": cik,
                        "reason": skip_reason,
                    }
                )
            facts_upserted_before = stats["facts_upserted"]
            if args.apply:
                apply_company_financials(
                    connection,
                    target,
                    records,
                    collected_at,
                    stats,
                    skipped_facts,
                )
            stats["processed_companies"] += 1
            processed_companies.append(
                {
                    "index": index,
                    "ticker": target.get("ticker"),
                    "cik": cik,
                    "company_id": target.get("company_id"),
                    "status": status,
                    "reason": skip_reason,
                    "source_objects": len(records["source_object"]),
                    "filings_seen": len(records["filing"]),
                    "facts_seen": len(records["financial_fact"]),
                    "facts_upserted": stats["facts_upserted"] - facts_upserted_before,
                    "started_at": started_at,
                    "finished_at": datetime.now(timezone.utc),
                }
            )
        except Exception as error:
            if connection is not None:
                connection.rollback()
            stats["failed_companies"] += 1
            failed_companies.append(
                {
                    "ticker": target.get("ticker"),
                    "cik": cik,
                    "error": str(error),
                }
            )
            processed_companies.append(
                {
                    "index": index,
                    "ticker": target.get("ticker"),
                    "cik": cik,
                    "company_id": target.get("company_id"),
                    "status": "FAILED",
                    "reason": type(error).__name__,
                    "error": str(error),
                    "started_at": started_at,
                    "finished_at": datetime.now(timezone.utc),
                }
            )
            LOGGER.exception("CIK=%s failed: %s", cik, error)

        LOGGER.info(
            "progress=%d/%d cik=%s processed=%d failed=%d facts_upserted=%d",
            index,
            len(targets),
            cik,
            stats["processed_companies"],
            stats["failed_companies"],
            stats["facts_upserted"],
        )

    return stats, failed_companies, skipped_facts, skipped_companies, processed_companies


def validate_args(args):
    """서로 충돌하거나 위험한 CLI 옵션을 실행 전에 검증한다."""
    if args.limit is not None and args.limit <= 0:
        raise ValueError("limit must be positive")
    if args.batch_size is not None and args.batch_size <= 0:
        raise ValueError("batch-size must be positive")
    if args.max_batches is not None and args.max_batches <= 0:
        raise ValueError("max-batches must be positive")
    if args.request_interval < 0:
        raise ValueError("request-interval must not be negative")
    if args.batch_sleep_seconds < 0:
        raise ValueError("batch-sleep-seconds must not be negative")
    if args.auto_batches and not args.matched_companies:
        raise ValueError("--auto-batches currently requires --matched-companies")


def execute_batch(client, storage, connection, args):
    """현재 start_after_cik와 limit 기준으로 한 번의 백필 배치를 실행한다."""
    targets = load_target_companies(connection, args)
    if connection is not None:
        connection.rollback()
    if not targets:
        return None

    (
        stats,
        failed_companies,
        skipped_facts,
        skipped_companies,
        processed_companies,
    ) = run_backfill(
        client,
        storage,
        connection,
        targets,
        args,
    )
    (
        summary_path,
        failed_path,
        skipped_path,
        skipped_companies_path,
        processed_path,
    ) = write_reports(
        args.output_dir,
        stats,
        failed_companies,
        skipped_facts,
        skipped_companies,
        processed_companies,
    )
    return {
        "targets": targets,
        "stats": stats,
        "summary_path": summary_path,
        "failed_path": failed_path,
        "skipped_path": skipped_path,
        "skipped_companies_path": skipped_companies_path,
        "processed_path": processed_path,
        "processed_companies": processed_companies,
    }


def print_batch_result(args, result, batch_number=None):
    """CLI 사용자가 다음 커서와 리포트 위치를 바로 확인할 수 있게 출력한다."""
    mode = "APPLY" if args.apply else "DRY_RUN"
    prefix = f"batch={batch_number} " if batch_number is not None else ""
    print(f"{prefix}mode={mode} selected={len(result['targets'])}")
    stats = result["stats"]
    for key in sorted(stats):
        print(f"{prefix}{key}={stats[key]}")
    print(f"{prefix}summary_report={result['summary_path']}")
    if result["failed_path"]:
        print(f"{prefix}failed_report={result['failed_path']}")
    if result["skipped_path"]:
        print(f"{prefix}skipped_report={result['skipped_path']}")
    if result["skipped_companies_path"]:
        print(f"{prefix}skipped_companies_report={result['skipped_companies_path']}")
    if result["processed_path"]:
        print(f"{prefix}processed_companies_report={result['processed_path']}")
    if result["processed_companies"]:
        print(f"{prefix}next_start_after_cik={result['processed_companies'][-1]['cik']}")


def run_auto_batches(client, storage, connection, args):
    """CIK 커서를 자동으로 넘기며 대상 기업이 없어질 때까지 배치 백필을 반복한다."""
    batch_size = args.batch_size or args.limit or 100
    original_limit = args.limit
    args.limit = batch_size
    batch_number = 0
    exit_code = 0

    try:
        while True:
            if args.max_batches is not None and batch_number >= args.max_batches:
                print(f"Reached max_batches={args.max_batches}.")
                break

            batch_number += 1
            LOGGER.info(
                "starting auto batch=%d start_after_cik=%s limit=%d",
                batch_number,
                normalize_cik(args.start_after_cik),
                batch_size,
            )
            result = execute_batch(client, storage, connection, args)
            if result is None:
                print("No SEC financial targets selected.")
                break

            print_batch_result(args, result, batch_number=batch_number)
            stats = result["stats"]
            if stats["failed_companies"]:
                exit_code = 2

            processed_companies = result["processed_companies"]
            if not processed_companies:
                print("No companies were attempted in the last batch. Stopping.")
                break

            if (
                stats["failed_companies"] == len(processed_companies)
                and stats["processed_companies"] == 0
            ):
                print("All companies in the last batch failed. Stopping auto batches.")
                break

            args.start_after_cik = processed_companies[-1]["cik"]
            if len(result["targets"]) < batch_size:
                print("Last batch was smaller than batch_size. Backfill range is exhausted.")
                break

            if args.batch_sleep_seconds:
                time.sleep(args.batch_sleep_seconds)
    finally:
        args.limit = original_limit

    return exit_code


def main():
    """환경 변수를 로드하고 SEC 재무 백필 CLI를 실행한다."""
    load_dotenv()
    args = parse_args()
    validate_args(args)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    connection = connect_database_if_needed(args)
    try:
        storage = LocalPipelineStorage(Path(args.output_dir))
        client = SECClient(
            require_user_agent(),
            min_request_interval=args.request_interval,
        )
        if args.auto_batches:
            exit_code = run_auto_batches(client, storage, connection, args)
            if exit_code:
                raise SystemExit(exit_code)
            return

        result = execute_batch(client, storage, connection, args)
        if result is None:
            print("No SEC financial targets selected.")
            return
        print_batch_result(args, result)
        if result["stats"]["failed_companies"]:
            raise SystemExit(2)
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
