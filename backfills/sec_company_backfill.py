import argparse
import logging
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

from collectors.common import (
    LocalPipelineStorage,
    build_target_record_key,
    transformed_record_hash,
)
from collectors.sec_client import SECClient
from transformers.sec_transformer import transform_company


LOGGER = logging.getLogger("sec-company-backfill")
TRANSFORMER_NAME = "sec-company-backfill"
TRANSFORMER_VERSION = "1.0.1"

SEC_EXCHANGE_MAP = {
    "NASDAQ": "NASDAQ",
    "NYSE": "NYSE",
    "NYSE AMERICAN": "NYSE_AMERICAN",
}


def parse_args():
    """SEC 대상 범위와 DB 반영·재개 옵션을 명령행 인자로 받는다."""
    parser = argparse.ArgumentParser(
        description="Collect SEC submissions and backfill company domain rows."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes. Without this flag records are fetched and validated only.",
    )
    parser.add_argument(
        "--cik",
        action="append",
        help="Process one CIK. Repeat the option to process multiple companies.",
    )
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process every CIK in SEC company_tickers.json.",
    )
    parser.add_argument(
        "--matched-assets-only",
        action="store_true",
        help=(
            "Process only SEC ticker+exchange rows that exactly match current "
            "KIS assets in the database."
        ),
    )
    parser.add_argument("--start-after-cik", default="0000000000")
    parser.add_argument("--request-interval", type=float, default=0.2)
    parser.add_argument(
        "--output-dir",
        default="pipeline_output/sec_company_backfill",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def normalize_cik(value):
    """숫자 또는 문자열 CIK를 SEC API가 요구하는 10자리 문자열로 만든다."""
    digits = re.sub(r"\D", "", str(value or ""))
    if not digits or len(digits) > 10:
        raise ValueError(f"invalid CIK: {value!r}")
    return digits.zfill(10)


def normalize_alias(value):
    """SEC 기업명을 중복 판정과 검색에 사용할 안정적인 Alias 키로 변환한다."""
    return re.sub(r"[^0-9A-Za-z가-힣]+", "", (value or "").casefold()).strip()


def normalize_ticker(value):
    """자동 매칭에서 기호 변형 없이 대소문자와 바깥 공백만 정규화한다."""
    return (value or "").strip().upper()


def normalize_sec_exchange(value):
    """SEC 거래소명을 KIS Asset에 저장한 내부 표준 거래소 코드로 변환한다."""
    normalized = " ".join((value or "").strip().upper().split())
    return SEC_EXCHANGE_MAP.get(normalized)


def normalize_alias_period(from_value, to_value):
    """Convert SEC alias timestamps to an ordered inclusive date range."""
    try:
        valid_from = (
            datetime.fromisoformat(str(from_value).replace("Z", "+00:00")).date()
            if from_value
            else None
        )
        valid_to = (
            datetime.fromisoformat(str(to_value).replace("Z", "+00:00")).date()
            if to_value
            else None
        )
    except (TypeError, ValueError):
        LOGGER.warning(
            "invalid SEC former-name period from=%r to=%r; storing no period",
            from_value,
            to_value,
        )
        return None, None, False

    was_reversed = (
        valid_from is not None
        and valid_to is not None
        and valid_from > valid_to
    )
    if was_reversed:
        valid_from, valid_to = valid_to, valid_from
    return valid_from, valid_to, was_reversed


def require_user_agent():
    """SEC 정책에 필요한 연락처 포함 User-Agent를 환경에서 읽는다."""
    value = os.getenv("SEC_USER_AGENT")
    if not value:
        raise RuntimeError("SEC_USER_AGENT is required in .env or the environment")
    return value


def require_database_url():
    """실제 반영 모드에서 사용할 PostgreSQL 접속 문자열을 환경에서 읽는다."""
    value = os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError(
            "DATABASE_URL is required with --apply, for example "
            "postgresql://postgres:<password>@localhost:55432/trader"
        )
    return value


def parse_sec_exchange_rows(payload):
    """SEC fields+data 배열을 이름 기반 딕셔너리 행으로 변환한다."""
    fields = payload.get("fields") or []
    data = payload.get("data") or []
    required = {"cik", "ticker", "exchange"}
    if not required.issubset(fields):
        raise ValueError(
            "SEC company_tickers_exchange.json is missing required fields: "
            f"{sorted(required - set(fields))}"
        )
    return [dict(zip(fields, row)) for row in data]


def fetch_kis_asset_identities(connection):
    """현재 KIS Provider Symbol과 내부 표준 거래소의 정확 매칭 키를 조회한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT aps.provider_symbol, a.exchange
            FROM asset_provider_symbol aps
            JOIN asset a ON a.id = aps.asset_id
            WHERE aps.provider = 'KIS'
              AND aps.is_current = TRUE
              AND a.active = TRUE
            """
        )
        return {
            (normalize_ticker(ticker), exchange)
            for ticker, exchange in cursor.fetchall()
        }


def match_sec_ciks_to_kis_assets(payload, kis_identities):
    """SEC ticker+거래소와 KIS 현재 Asset이 정확히 일치하는 고유 CIK를 선택한다."""
    matched_ciks = set()
    stats = Counter()
    for row in parse_sec_exchange_rows(payload):
        stats["sec_rows"] += 1
        ticker = normalize_ticker(row.get("ticker"))
        exchange = normalize_sec_exchange(row.get("exchange"))
        if not ticker or exchange is None:
            stats["unsupported_or_empty"] += 1
            continue
        if (ticker, exchange) not in kis_identities:
            stats["not_in_kis"] += 1
            continue
        matched_ciks.add(normalize_cik(row.get("cik")))
        stats["matched_rows"] += 1
    stats["matched_ciks"] = len(matched_ciks)
    return matched_ciks, stats


def select_ciks(client, args, connection=None):
    """명시 CIK, 전체 SEC 또는 KIS 교집합에서 정렬된 실행 대상을 만든다."""
    if args.cik:
        values = {normalize_cik(value) for value in args.cik}
    elif args.matched_assets_only:
        if connection is None:
            raise RuntimeError("database connection is required for asset matching")
        payload = client.get_company_tickers_with_exchange()
        kis_identities = fetch_kis_asset_identities(connection)
        values, match_stats = match_sec_ciks_to_kis_assets(
            payload, kis_identities
        )
        LOGGER.info(
            "SEC-KIS intersection sec_rows=%d kis_identities=%d "
            "matched_rows=%d matched_ciks=%d unsupported_or_empty=%d not_in_kis=%d",
            match_stats["sec_rows"],
            len(kis_identities),
            match_stats["matched_rows"],
            match_stats["matched_ciks"],
            match_stats["unsupported_or_empty"],
            match_stats["not_in_kis"],
        )
    else:
        if not args.all and args.limit is None:
            raise ValueError(
                "specify --cik, --limit, --all, or --matched-assets-only"
            )
        payload = client.get_company_tickers()
        values = {
            normalize_cik(item.get("cik_str"))
            for item in payload.values()
            if item.get("cik_str") is not None
        }

    start_after = normalize_cik(args.start_after_cik)
    selected = [cik for cik in sorted(values) if cik > start_after]
    if args.limit is not None:
        selected = selected[: args.limit]
    return selected


def company_aliases(submissions):
    """현재 SEC 법인명과 과거 법인명을 company_alias 적재 레코드로 만든다."""
    records = []
    current_name = (submissions.get("name") or "").strip()
    if current_name:
        records.append(
            {
                "alias": current_name,
                "normalized_alias": normalize_alias(current_name),
                "alias_type": "LEGAL_NAME",
                "valid_from": None,
                "valid_to": None,
                "period_corrected": False,
            }
        )

    for former in submissions.get("formerNames") or []:
        name = (former.get("name") or "").strip()
        if not name:
            continue
        valid_from, valid_to, period_corrected = normalize_alias_period(
            former.get("from"), former.get("to")
        )
        if period_corrected:
            LOGGER.warning(
                "normalized reversed SEC former-name period cik=%s name=%r "
                "source_from=%r source_to=%r",
                submissions.get("cik"),
                name,
                former.get("from"),
                former.get("to"),
            )
        records.append(
            {
                "alias": name,
                "normalized_alias": normalize_alias(name),
                "alias_type": "FORMER_NAME",
                "valid_from": valid_from,
                "valid_to": valid_to,
                "period_corrected": period_corrected,
            }
        )
    return records


def upsert_source_object(cursor, source):
    """로컬에 보존한 SEC 원본 메타데이터를 source_object에 멱등 저장한다."""
    cursor.execute(
        """
        INSERT INTO source_object (
            provider, data_type, external_id, storage_backend, storage_key, object_uri, object_size,
            content_hash, dedupe_key, collected_at, processing_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PROCESSED')
        ON CONFLICT (dedupe_key)
        DO UPDATE SET
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


def upsert_company(cursor, company):
    """CIK 자연키로 SEC 기업 메타데이터를 멱등 upsert한다."""
    cursor.execute(
        """
        INSERT INTO company (
            cik, company_name, sic_code, sic_description,
            fiscal_year_end, country_code, entity_type,
            collected_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (cik)
        DO UPDATE SET
            company_name = EXCLUDED.company_name,
            sic_code = EXCLUDED.sic_code,
            sic_description = EXCLUDED.sic_description,
            fiscal_year_end = EXCLUDED.fiscal_year_end,
            country_code = EXCLUDED.country_code,
            entity_type = EXCLUDED.entity_type,
            collected_at = EXCLUDED.collected_at,
            updated_at = EXCLUDED.updated_at
        RETURNING id
        """,
        (
            company["cik"],
            company["company_name"],
            company["sic_code"],
            company["sic_description"],
            company["fiscal_year_end"],
            company["country_code"],
            company["entity_type"],
            company["collected_at"],
            company["updated_at"],
        ),
    )
    return cursor.fetchone()[0]


def upsert_company_aliases(cursor, company_id, aliases):
    """현재·과거 SEC 법인명을 유효기간과 함께 중복 없이 저장한다."""
    for alias in aliases:
        cursor.execute(
            """
            INSERT INTO company_alias (
                company_id, alias, normalized_alias, alias_type,
                language, is_ambiguous, valid_from, valid_to, source
            ) VALUES (%s, %s, %s, %s, 'en', FALSE, %s, %s, 'SEC')
            ON CONFLICT (company_id, normalized_alias, alias_type)
            DO UPDATE SET
                alias = EXCLUDED.alias,
                valid_from = COALESCE(EXCLUDED.valid_from, company_alias.valid_from),
                valid_to = COALESCE(EXCLUDED.valid_to, company_alias.valid_to)
            """,
            (
                company_id,
                alias["alias"],
                alias["normalized_alias"],
                alias["alias_type"],
                alias["valid_from"],
                alias["valid_to"],
            ),
        )


def upsert_lineage(cursor, source_object_id, company_id, company, processed_at):
    """SEC Submissions 원본과 company 결과의 Transformer 버전을 연결한다."""
    target_key = build_target_record_key("company", {"cik": company["cik"]})
    cursor.execute(
        """
        INSERT INTO record_lineage (
            source_object_id, target_table, target_record_id, target_record_key,
            transformer_name, transformer_version, output_hash, processed_at
        ) VALUES (%s, 'company', %s, %s, %s, %s, %s, %s)
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
            company_id,
            target_key,
            TRANSFORMER_NAME,
            TRANSFORMER_VERSION,
            transformed_record_hash(company),
            processed_at,
        ),
    )


def apply_company(connection, source, company, aliases, processed_at):
    """원본·Company·Alias·Lineage를 한 기업 단위 트랜잭션에 저장한다."""
    with connection.cursor() as cursor:
        source_object_id = upsert_source_object(cursor, source)
        company_id = upsert_company(cursor, company)
        upsert_company_aliases(cursor, company_id, aliases)
        upsert_lineage(cursor, source_object_id, company_id, company, processed_at)
    connection.commit()
    return company_id


def run_backfill(client, storage, connection, ciks, apply_changes):
    """CIK별 SEC Submissions를 수집하고 실패를 격리해 계속 처리한다."""
    stats = Counter()
    last_cik = None
    for index, cik in enumerate(ciks, start=1):
        last_cik = cik
        try:
            collected_at = datetime.now(timezone.utc)
            submissions = client.get_submissions(cik)
            source = storage.write_raw_json(
                "SEC", "SUBMISSIONS", cik, submissions, collected_at
            )
            company = transform_company(
                submissions, source["object_uri"], collected_at
            )
            if not company.get("company_name"):
                raise ValueError("SEC submissions does not contain company name")
            aliases = company_aliases(submissions)
            stats["fetched"] += 1
            stats["aliases"] += len(aliases)
            stats["alias_periods_corrected"] += sum(
                alias["period_corrected"] for alias in aliases
            )

            if apply_changes:
                apply_company(
                    connection, source, company, aliases, collected_at
                )
            stats["processed"] += 1
        except Exception as error:
            if connection is not None:
                connection.rollback()
            stats["failed"] += 1
            LOGGER.exception("CIK=%s failed: %s", cik, error)

        LOGGER.info(
            "progress=%d/%d cik=%s processed=%d failed=%d",
            index,
            len(ciks),
            cik,
            stats["processed"],
            stats["failed"],
        )
    return stats, last_cik


def main():
    """환경을 로드하고 SEC Company dry-run 또는 실제 백필을 실행한다."""
    load_dotenv()
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.limit is not None and args.limit <= 0:
        raise ValueError("limit must be positive")
    if args.request_interval < 0:
        raise ValueError("request-interval must not be negative")
    if args.all and args.matched_assets_only:
        raise ValueError("--all and --matched-assets-only cannot be used together")

    client = SECClient(
        require_user_agent(), min_request_interval=args.request_interval
    )
    connection = None
    if args.apply or args.matched_assets_only:
        connection = psycopg2.connect(require_database_url())
        connection.autocommit = False

    ciks = select_ciks(client, args, connection)
    if connection is not None:
        # 대상 선택 SELECT가 장시간 SEC 호출 동안 열린 트랜잭션으로 남지 않게 정리한다.
        connection.rollback()
    if not ciks:
        print("No SEC companies selected.")
        if connection is not None:
            connection.close()
        return

    storage = LocalPipelineStorage(Path(args.output_dir))

    try:
        stats, last_cik = run_backfill(
            client, storage, connection, ciks, args.apply
        )
    finally:
        if connection is not None:
            connection.close()

    mode = "APPLY" if args.apply else "DRY_RUN"
    print(f"mode={mode} selected={len(ciks)} last_cik={last_cik}")
    for key in sorted(stats):
        print(f"{key}={stats[key]}")
    if stats["failed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
