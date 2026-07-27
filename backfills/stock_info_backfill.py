import argparse
import logging
import os
import re
from collections import Counter
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv

#python -m backfills.stock_info_backfill `
#  --apply `
#  --batch-size 500
LOGGER = logging.getLogger("stock-info-backfill")

EXCHANGE_MAP = {
    "NAS": "NASDAQ",
    "NYS": "NYSE",
    "AMS": "NYSE_AMERICAN",
}

ETP_TYPE_MAP = {
    1: "ETF",
    2: "ETN",
    3: "ETC",
    4: "ETP_OTHER",
    5: "VIX_ETF",
    6: "VIX_ETN",
}


def parse_args():
    """백필 범위와 실제 반영 여부를 명령행 인자로 받는다."""
    parser = argparse.ArgumentParser(
        description="Backfill legacy stock_info rows into asset domain tables."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes. Without this flag the command only validates rows.",
    )
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--start-after-id", type=int, default=0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def require_database_url():
    """환경변수에서 PostgreSQL 접속 문자열을 읽고 없으면 안전하게 중단한다."""
    value = os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError(
            "DATABASE_URL is required, for example "
            "postgresql://postgres:<password>@localhost:55432/trader"
        )
    return value


def normalize_alias(value):
    """한글·영문 종목명을 대소문자와 구두점에 덜 민감한 검색 키로 변환한다."""
    normalized = re.sub(r"[^0-9A-Za-z가-힣]+", "", (value or "").casefold())
    return normalized.strip()


def normalize_exchange(excd):
    """KIS 제공자 거래소 코드를 내부 표준 거래소 코드로 변환한다."""
    code = (excd or "").strip().upper()
    if code not in EXCHANGE_MAP:
        raise ValueError(f"unsupported KIS exchange code: {code or '<empty>'}")
    return EXCHANGE_MAP[code]


def resolve_asset_type(stock_type, classification_code):
    """KIS 증권유형과 ETP 구분코드를 서비스 Asset 유형으로 변환한다."""
    if stock_type == 1:
        return "INDEX"
    if stock_type == 2:
        return "EQUITY"
    if stock_type == 3:
        return ETP_TYPE_MAP.get(classification_code, "ETP_OTHER")
    if stock_type == 4:
        return "WARRANT"
    return "UNKNOWN"


def transform_stock_info(row):
    """stock_info 한 행을 Asset과 Alias·Provider Symbol 적재 값으로 변환한다."""
    source_id, excd, symb, name, ename, stock_type, _gics, classification = row
    ticker = (symb or "").strip().upper()
    if not ticker:
        raise ValueError("ticker is empty")

    display_name = (name or "").strip() or (ename or "").strip() or ticker
    exchange = normalize_exchange(excd)
    aliases = []
    for alias, alias_type, language, primary in (
        (name, "KOREAN_NAME", "ko", True),
        (ename, "ENGLISH_NAME", "en", False),
    ):
        alias = (alias or "").strip()
        normalized = normalize_alias(alias)
        if alias and normalized:
            aliases.append(
                {
                    "alias": alias,
                    "normalized_alias": normalized,
                    "alias_type": alias_type,
                    "language": language,
                    "is_primary": primary,
                }
            )

    return {
        "source_id": source_id,
        "provider_exchange_code": (excd or "").strip().upper(),
        "ticker": ticker,
        "name": display_name,
        "exchange": exchange,
        "asset_type": resolve_asset_type(stock_type, classification),
        "currency": "USD",
        "timezone": "America/New_York",
        "aliases": aliases,
    }


def fetch_stock_info_batch(cursor, last_id, batch_size, remaining):
    """PK keyset pagination으로 다음 stock_info 묶음을 읽는다."""
    size = min(batch_size, remaining) if remaining is not None else batch_size
    cursor.execute(
        """
        SELECT id, excd, symb, name, ename, stock_type, gics, classification_code
        FROM stock_info
        WHERE id > %s
        ORDER BY id
        LIMIT %s
        """,
        (last_id, size),
    )
    return cursor.fetchall()


def upsert_asset(cursor, record):
    """활성 거래소·ticker 자연키로 Asset을 멱등 upsert하고 내부 ID를 반환한다."""
    cursor.execute(
        """
        INSERT INTO asset (
            company_id, asset_type, ticker, name, exchange,
            currency, timezone, active
        ) VALUES (NULL, %s, %s, %s, %s, %s, %s, TRUE)
        ON CONFLICT (exchange, ticker) WHERE active = TRUE
        DO UPDATE SET
            name = EXCLUDED.name,
            asset_type = CASE
                WHEN asset.asset_type IN ('UNKNOWN', 'EQUITY')
                    THEN EXCLUDED.asset_type
                ELSE asset.asset_type
            END,
            currency = EXCLUDED.currency,
            timezone = EXCLUDED.timezone
        RETURNING id
        """,
        (
            record["asset_type"],
            record["ticker"],
            record["name"],
            record["exchange"],
            record["currency"],
            record["timezone"],
        ),
    )
    return cursor.fetchone()[0]


def upsert_aliases(cursor, asset_id, aliases):
    """한글·영문 Asset Alias를 중복 없이 저장한다."""
    for alias in aliases:
        cursor.execute(
            """
            INSERT INTO asset_alias (
                asset_id, alias, normalized_alias, alias_type,
                language, source, is_primary
            ) VALUES (%s, %s, %s, %s, %s, 'KIS', %s)
            ON CONFLICT (asset_id, normalized_alias, alias_type)
            DO UPDATE SET
                alias = EXCLUDED.alias,
                language = EXCLUDED.language,
                is_primary = EXCLUDED.is_primary
            """,
            (
                asset_id,
                alias["alias"],
                alias["normalized_alias"],
                alias["alias_type"],
                alias["language"],
                alias["is_primary"],
            ),
        )


def upsert_provider_symbol(cursor, asset_id, record, collected_at):
    """KIS API 호출에 필요한 거래소·ticker와 레거시 원본 ID를 저장한다."""
    cursor.execute(
        """
        INSERT INTO asset_provider_symbol (
            asset_id, provider, provider_exchange_code, provider_symbol,
            source_record_id, is_current, collected_at
        ) VALUES (%s, 'KIS', %s, %s, %s, TRUE, %s)
        ON CONFLICT (provider, provider_exchange_code, provider_symbol)
            WHERE is_current = TRUE
        DO UPDATE SET
            asset_id = EXCLUDED.asset_id,
            source_record_id = EXCLUDED.source_record_id,
            collected_at = EXCLUDED.collected_at
        """,
        (
            asset_id,
            record["provider_exchange_code"],
            record["ticker"],
            str(record["source_id"]),
            collected_at,
        ),
    )


def apply_record(cursor, record, collected_at):
    """Asset·Alias·Provider Symbol을 같은 DB 트랜잭션 단위에 저장한다."""
    asset_id = upsert_asset(cursor, record)
    upsert_aliases(cursor, asset_id, record["aliases"])
    upsert_provider_symbol(cursor, asset_id, record, collected_at)
    return asset_id


def run_backfill(connection, args):
    """stock_info를 keyset 배치로 검증하거나 신규 Asset 스키마에 반영한다."""
    stats = Counter()
    last_id = args.start_after_id
    remaining = args.limit
    collected_at = datetime.now(timezone.utc)

    while remaining is None or remaining > 0:
        with connection.cursor() as cursor:
            rows = fetch_stock_info_batch(
                cursor, last_id, args.batch_size, remaining
            )
        if not rows:
            break

        for row in rows:
            last_id = row[0]
            stats["read"] += 1
            try:
                record = transform_stock_info(row)
                stats[f"asset_type:{record['asset_type']}"] += 1
                stats[f"exchange:{record['exchange']}"] += 1
                if args.apply:
                    with connection.cursor() as cursor:
                        cursor.execute("SAVEPOINT stock_info_row")
                        try:
                            apply_record(cursor, record, collected_at)
                        except Exception:
                            cursor.execute("ROLLBACK TO SAVEPOINT stock_info_row")
                            raise
                        finally:
                            cursor.execute("RELEASE SAVEPOINT stock_info_row")
                stats["processed"] += 1
            except Exception as error:
                stats["failed"] += 1
                LOGGER.exception("stock_info id=%s failed: %s", row[0], error)

        if args.apply:
            connection.commit()
        if remaining is not None:
            remaining -= len(rows)
        LOGGER.info(
            "progress read=%d processed=%d failed=%d last_id=%d",
            stats["read"],
            stats["processed"],
            stats["failed"],
            last_id,
        )

    return stats, last_id


def main():
    """환경을 로드하고 dry-run 또는 실제 stock_info 백필을 실행한다."""
    load_dotenv()
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.batch_size <= 0 or (args.limit is not None and args.limit <= 0):
        raise ValueError("batch-size and limit must be positive")

    connection = psycopg2.connect(require_database_url())
    connection.autocommit = False
    try:
        stats, last_id = run_backfill(connection, args)
        if not args.apply:
            connection.rollback()
    finally:
        connection.close()

    mode = "APPLY" if args.apply else "DRY_RUN"
    print(f"mode={mode} last_id={last_id}")
    for key in sorted(stats):
        print(f"{key}={stats[key]}")
    if stats["failed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
