import argparse
import csv
import json
import logging
import os
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

from collectors.common import content_hash
from collectors.kis_client import KISAPIError, KISClient


LOGGER = logging.getLogger("price-bar-invalid-repair")
REPORT_ROOT = Path("pipeline_output") / "reports" / "price_bar_invalid"
RAW_ROOT = Path("pipeline_output") / "raw" / "kis" / "price_bar_repair"
DEFAULT_SOURCE = "KIS"
DEFAULT_INTERVAL = "1D"


@dataclass(frozen=True)
class InvalidBar:
    """레거시 stock에서 검증에 실패한 일봉 한 건의 복구 식별 정보를 보관한다."""

    asset_id: int
    exchange_code: str
    symbol: str
    trade_date: date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: Decimal | None
    reasons: tuple[str, ...]


@dataclass
class RepairRange:
    """한 번의 KIS 기간 조회로 함께 복구할 invalid 일봉 묶음이다."""

    asset_id: int
    exchange_code: str
    symbol: str
    start_date: date
    end_date: date
    items: list[InvalidBar]


def parse_args():
    """보고서 생성, API 조회, 실제 반영 범위를 제어하는 CLI 옵션을 읽는다."""
    parser = argparse.ArgumentParser(
        description="Re-fetch and repair legacy stock rows with invalid OHLC values."
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Call the KIS API. Without this flag only export invalid rows.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Upsert validated KIS rows into price_bar. Requires --fetch.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Allow API calls for every selected invalid symbol.",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        help="Restrict to a ticker. Repeat to select multiple tickers.",
    )
    parser.add_argument("--start", help="Inclusive date in YYYY-MM-DD format.")
    parser.add_argument("--end", help="Inclusive date in YYYY-MM-DD format.")
    parser.add_argument(
        "--max-gap-days",
        type=int,
        default=30,
        help="Keep invalid dates in one API range when their gap is this small.",
    )
    parser.add_argument(
        "--max-range-days",
        type=int,
        default=366,
        help="Maximum calendar span of one KIS request group.",
    )
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--adjusted", action="store_true")
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--interval", default=DEFAULT_INTERVAL)
    parser.add_argument("--currency", default="USD")
    parser.add_argument("--report-dir", default=str(REPORT_ROOT))
    parser.add_argument("--raw-dir", default=str(RAW_ROOT))
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def require_database_url():
    """공유 PostgreSQL에 연결하기 위한 DATABASE_URL을 반환한다."""
    value = os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError("DATABASE_URL is required in .env or the environment")
    return value


def invalid_reasons(open_value, high, low, close, volume):
    """OHLCV 값이 어떤 불변식을 위반했는지 사람이 읽을 수 있게 분류한다."""
    reasons = []
    prices = (open_value, high, low, close)
    if any(value is None for value in prices):
        reasons.append("MISSING_PRICE")
        return tuple(reasons)
    if high < max(prices):
        reasons.append("HIGH_BELOW_PRICE")
    if low > min(prices):
        reasons.append("LOW_ABOVE_PRICE")
    if volume is not None and volume < 0:
        reasons.append("NEGATIVE_VOLUME")
    return tuple(reasons)


def is_valid_bar(bar):
    """KIS 재조회 결과가 price_bar의 필수값과 OHLC 불변식을 만족하는지 검사한다."""
    return not invalid_reasons(
        bar.get("open"),
        bar.get("high"),
        bar.get("low"),
        bar.get("close"),
        bar.get("volume"),
    )


def normalize_symbols(symbols):
    """중복과 공백을 제거한 대문자 ticker 목록을 만든다."""
    return sorted(
        {
            symbol.strip().upper()
            for symbol in (symbols or [])
            if symbol and symbol.strip()
        }
    )


def resolve_report_dir(base_dir, symbols=None, start=None, end=None):
    """필터 실행이 전체 보고서를 덮어쓰지 않도록 실행 범위별 경로를 만든다."""
    normalized_symbols = normalize_symbols(symbols)
    if not normalized_symbols and start is None and end is None:
        return Path(base_dir)

    symbol_part = "-".join(normalized_symbols) if normalized_symbols else "ALL"
    start_part = start.isoformat() if start else "MIN"
    end_part = end.isoformat() if end else "MAX"
    return Path(base_dir) / f"{symbol_part}_{start_part}_{end_part}"


def fetch_invalid_bars(connection, provider, symbols=None, start=None, end=None):
    """레거시 stock에서 Asset 매핑은 됐지만 OHLCV 검증에 실패한 행을 조회한다."""
    predicates = []
    params = [provider]
    normalized_symbols = normalize_symbols(symbols)
    if normalized_symbols:
        predicates.append("s.symb = ANY(%s)")
        params.append(normalized_symbols)
    if start:
        predicates.append("s.timestamp >= %s")
        params.append(datetime.combine(start, time.min, tzinfo=timezone.utc))
    if end:
        predicates.append("s.timestamp < %s")
        params.append(
            datetime.combine(end + timedelta(days=1), time.min, tzinfo=timezone.utc)
        )
    extra_where = "".join(f" AND {predicate}" for predicate in predicates)

    query = f"""
        SELECT
            aps.asset_id,
            aps.provider_exchange_code,
            aps.provider_symbol,
            (s.timestamp AT TIME ZONE 'UTC')::date AS trade_date,
            s.open,
            s.high,
            s.low,
            s.close,
            s.volume
        FROM stock s
        JOIN asset_provider_symbol aps
          ON aps.provider = %s
         AND aps.provider_symbol = s.symb
         AND aps.is_current = TRUE
        WHERE NOT (
            s.open IS NOT NULL
            AND s.high IS NOT NULL
            AND s.low IS NOT NULL
            AND s.close IS NOT NULL
            AND s.high >= GREATEST(s.open, s.close, s.low)
            AND s.low <= LEAST(s.open, s.close, s.high)
            AND (s.volume IS NULL OR s.volume >= 0)
        )
        {extra_where}
        ORDER BY aps.provider_symbol, trade_date
    """
    invalid = []
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        for row in cursor:
            reasons = invalid_reasons(row[4], row[5], row[6], row[7], row[8])
            invalid.append(
                InvalidBar(
                    asset_id=row[0],
                    exchange_code=row[1],
                    symbol=row[2],
                    trade_date=row[3],
                    open=row[4],
                    high=row[5],
                    low=row[6],
                    close=row[7],
                    volume=row[8],
                    reasons=reasons,
                )
            )
    return invalid


def group_repair_ranges(items, max_gap_days=30, max_range_days=366):
    """같은 종목의 가까운 invalid 날짜를 제한된 기간 조회 단위로 묶는다."""
    ranges = []
    current = None
    for item in sorted(
        items,
        key=lambda value: (
            value.asset_id,
            value.exchange_code,
            value.symbol,
            value.trade_date,
        ),
    ):
        same_identity = (
            current is not None
            and current.asset_id == item.asset_id
            and current.exchange_code == item.exchange_code
            and current.symbol == item.symbol
        )
        close_enough = (
            same_identity
            and (item.trade_date - current.end_date).days <= max_gap_days
            and (item.trade_date - current.start_date).days <= max_range_days
        )
        if not close_enough:
            current = RepairRange(
                asset_id=item.asset_id,
                exchange_code=item.exchange_code,
                symbol=item.symbol,
                start_date=item.trade_date,
                end_date=item.trade_date,
                items=[item],
            )
            ranges.append(current)
            continue
        current.end_date = item.trade_date
        current.items.append(item)
    return ranges


def write_invalid_reports(items, report_dir):
    """invalid 상세 목록과 사유별·연도별·종목별 요약 CSV를 생성한다."""
    report_dir.mkdir(parents=True, exist_ok=True)
    details_path = report_dir / "price_bar_invalid_items.csv"
    summary_path = report_dir / "price_bar_invalid_summary.csv"

    with details_path.open("w", encoding="utf-8-sig", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(
            [
                "asset_id",
                "exchange_code",
                "symbol",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "invalid_reasons",
            ]
        )
        for item in items:
            writer.writerow(
                [
                    item.asset_id,
                    item.exchange_code,
                    item.symbol,
                    item.trade_date.isoformat(),
                    item.open,
                    item.high,
                    item.low,
                    item.close,
                    item.volume,
                    "|".join(item.reasons),
                ]
            )

    counts = Counter()
    for item in items:
        for reason in item.reasons:
            counts[(reason, item.trade_date.year, item.symbol)] += 1
    with summary_path.open("w", encoding="utf-8-sig", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(["invalid_reason", "year", "symbol", "row_count"])
        for (reason, year, symbol), row_count in sorted(counts.items()):
            writer.writerow([reason, year, symbol, row_count])
    return details_path, summary_path


def select_ranges(ranges, symbols, allow_all, limit_symbols):
    """명시적 ticker 또는 --all 승인 범위 안에서 실제 API 호출 대상을 선택한다."""
    normalized_symbols = normalize_symbols(symbols)
    if not normalized_symbols and not allow_all:
        raise ValueError("--fetch requires at least one --symbol or explicit --all")

    selected = [
        item
        for item in ranges
        if not normalized_symbols or item.symbol in normalized_symbols
    ]
    if limit_symbols:
        allowed = []
        for item in selected:
            if item.symbol not in allowed:
                allowed.append(item.symbol)
            if len(allowed) >= limit_symbols:
                break
        selected = [item for item in selected if item.symbol in set(allowed)]
    return selected


def write_raw_response(raw_dir, repair_range, result, adjusted):
    """감사와 장애 분석을 위해 KIS 재조회 원본 페이지를 JSON 파일로 보존한다."""
    symbol_dir = raw_dir / repair_range.exchange_code / repair_range.symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)
    collected_at = datetime.now(timezone.utc)
    path = symbol_dir / (
        f"{repair_range.start_date.isoformat()}_"
        f"{repair_range.end_date.isoformat()}_"
        f"{collected_at.strftime('%Y%m%dT%H%M%SZ')}.json"
    )
    payload = {
        "provider": "KIS",
        "exchange_code": repair_range.exchange_code,
        "symbol": repair_range.symbol,
        "start_date": repair_range.start_date.isoformat(),
        "end_date": repair_range.end_date.isoformat(),
        "adjusted": adjusted,
        "collected_at": collected_at.isoformat(),
        "pages": result["raw_pages"],
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


def build_repair_rows(repair_range, fetched_bars, collected_at):
    """원래 invalid였던 날짜와 정확히 일치하고 재검증을 통과한 KIS 행만 추린다."""
    invalid_by_date = {item.trade_date: item for item in repair_range.items}
    rows = []
    statuses = []
    fetched_by_date = {bar["date"]: bar for bar in fetched_bars}

    for trade_date, original in sorted(invalid_by_date.items()):
        fetched = fetched_by_date.get(trade_date)
        if fetched is None:
            statuses.append((original, "NOT_RETURNED", None))
            continue
        reasons = invalid_reasons(
            fetched["open"],
            fetched["high"],
            fetched["low"],
            fetched["close"],
            fetched["volume"],
        )
        if reasons:
            statuses.append((original, "STILL_INVALID", reasons))
            continue

        stable_payload = {
            "asset_id": repair_range.asset_id,
            "source": DEFAULT_SOURCE,
            "interval": DEFAULT_INTERVAL,
            "bar_time": trade_date.isoformat(),
            "open": str(fetched["open"]),
            "high": str(fetched["high"]),
            "low": str(fetched["low"]),
            "close": str(fetched["close"]),
            "volume": str(fetched["volume"]) if fetched["volume"] is not None else None,
            "currency": "USD",
        }
        rows.append(
            {
                "asset_id": repair_range.asset_id,
                "trade_date": trade_date,
                "open": fetched["open"],
                "high": fetched["high"],
                "low": fetched["low"],
                "close": fetched["close"],
                "volume": fetched["volume"],
                "collected_at": collected_at,
                "content_hash": content_hash(stable_payload),
            }
        )
        statuses.append((original, "RECOVERABLE", None))
    return rows, statuses


def upsert_price_bars(connection, rows, source, interval, currency, return_records=False):
    """검증된 복구 값을 price_bar 업무키로 멱등 UPSERT하고 반영 건수를 반환한다."""
    if not rows:
        return [] if return_records else 0
    if any(row.get("provider_adjusted", False) is not False for row in rows):
        raise ValueError("수정주가는 일반 가격 테이블에 저장할 수 없습니다.")
    values = [
        (
            row["asset_id"],
            source,
            interval,
            datetime.combine(row["trade_date"], time.min, tzinfo=timezone.utc),
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            None,
            row["volume"],
            currency,
            row["collected_at"],
            row["content_hash"],
            "VALID",
            "FULL_OHLC",
            row["collected_at"],
        )
        for row in rows
    ]
    query = """
        INSERT INTO price_bar (
            asset_id, source, "interval", bar_time,
            open, high, low, close, adjusted_close, volume,
            currency, collected_at, content_hash,
            quality_status, display_policy, quality_checked_at
        ) VALUES %s
        ON CONFLICT (asset_id, source, "interval", bar_time)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            currency = EXCLUDED.currency,
            collected_at = EXCLUDED.collected_at,
            content_hash = EXCLUDED.content_hash,
            quality_status = EXCLUDED.quality_status,
            display_policy = EXCLUDED.display_policy,
            quality_checked_at = EXCLUDED.quality_checked_at
    """
    if return_records:
        query += """
        RETURNING id, asset_id, source, "interval", bar_time, content_hash
        """
    with connection.cursor() as cursor:
        returned = execute_values(
            cursor,
            query,
            values,
            page_size=500,
            fetch=return_records,
        )
        mark_repaired_quality_issues(cursor, rows, source, interval)
    if return_records:
        return [
            {
                "id": row[0],
                "asset_id": row[1],
                "source": row[2],
                "interval": row[3],
                "bar_time": row[4],
                "content_hash": row[5],
            }
            for row in returned
        ]
    return len(rows)


def mark_repaired_quality_issues(cursor, rows, source, interval):
    """정상 재조회로 교체된 가격 행의 기존 품질 이슈를 해결 상태로 닫는다."""
    if not rows:
        return
    values = [
        (
            row["asset_id"],
            source,
            interval,
            datetime.combine(row["trade_date"], time.min, tzinfo=timezone.utc),
        )
        for row in rows
    ]
    query = """
        WITH repaired(asset_id, source, "interval", bar_time) AS (VALUES %s)
        UPDATE price_bar_quality_issue issue
        SET issue_status = 'RESOLVED',
            resolved_at = CURRENT_TIMESTAMP,
            last_seen_at = CURRENT_TIMESTAMP,
            resolution_source = repaired.source,
            resolution_note = 'Provider refetch returned a valid OHLCV bar'
        FROM repaired
        WHERE issue.asset_id = repaired.asset_id
          AND issue.source = repaired.source
          AND issue."interval" = repaired."interval"
          AND issue.bar_time = repaired.bar_time
          AND issue.issue_status <> 'RESOLVED'
    """
    execute_values(cursor, query, values, page_size=500)


def write_repair_report(statuses, report_dir):
    """API 조회 후 날짜별 복구 가능 여부와 잔여 실패 원인을 CSV로 기록한다."""
    path = report_dir / "price_bar_repair_results.csv"
    with path.open("w", encoding="utf-8-sig", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(
            [
                "asset_id",
                "exchange_code",
                "symbol",
                "trade_date",
                "original_reasons",
                "repair_status",
                "fetched_invalid_reasons",
            ]
        )
        for original, status, fetched_reasons in statuses:
            writer.writerow(
                [
                    original.asset_id,
                    original.exchange_code,
                    original.symbol,
                    original.trade_date.isoformat(),
                    "|".join(original.reasons),
                    status,
                    "|".join(fetched_reasons or ()),
                ]
            )
    return path


def run_repair(connection, client, ranges, args, report_dir, raw_dir):
    """선택한 범위를 순차 조회하고 범위별 트랜잭션으로 복구 결과를 반영한다."""
    if args.apply and args.adjusted:
        raise ValueError("수정주가 적재는 원문 계보를 보존하는 KIS worker를 사용하세요.")
    stats = Counter()
    all_statuses = []
    collected_at = datetime.now(timezone.utc)

    for index, repair_range in enumerate(ranges, start=1):
        LOGGER.info(
            "range=%d/%d symbol=%s exchange=%s start=%s end=%s invalid=%d",
            index,
            len(ranges),
            repair_range.symbol,
            repair_range.exchange_code,
            repair_range.start_date,
            repair_range.end_date,
            len(repair_range.items),
        )
        try:
            result = client.get_daily_prices(
                exchange_code=repair_range.exchange_code,
                symbol=repair_range.symbol,
                start_date=repair_range.start_date,
                end_date=repair_range.end_date,
                adjusted=args.adjusted,
                max_pages=args.max_pages,
            )
            write_raw_response(raw_dir, repair_range, result, args.adjusted)
            rows, statuses = build_repair_rows(
                repair_range,
                result["bars"],
                collected_at,
            )
            all_statuses.extend(statuses)
            stats.update(status for _item, status, _reasons in statuses)
            stats["fetched_rows"] += len(result["bars"])

            if args.apply:
                affected = upsert_price_bars(
                    connection,
                    rows,
                    args.source.strip().upper(),
                    args.interval.strip().upper(),
                    args.currency.strip().upper(),
                )
                connection.commit()
                stats["affected_rows"] += affected
            else:
                connection.rollback()
        except (KISAPIError, OSError, ValueError) as error:
            connection.rollback()
            stats["api_failed_ranges"] += 1
            LOGGER.exception(
                "KIS repair failed for %s/%s %s..%s: %s",
                repair_range.exchange_code,
                repair_range.symbol,
                repair_range.start_date,
                repair_range.end_date,
                error,
            )
            for item in repair_range.items:
                all_statuses.append((item, "API_FAILED", (str(error),)))

    result_path = write_repair_report(all_statuses, report_dir)
    return stats, result_path


def main():
    """invalid 보고서를 만들고 명시적으로 요청된 경우에만 KIS 복구를 수행한다."""
    load_dotenv()
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.apply and not args.fetch:
        raise ValueError("--apply requires --fetch")
    if args.max_gap_days < 0 or args.max_range_days <= 0:
        raise ValueError("range grouping days must be positive")
    if args.limit_symbols is not None and args.limit_symbols <= 0:
        raise ValueError("limit-symbols must be positive")

    start = date.fromisoformat(args.start) if args.start else None
    end = date.fromisoformat(args.end) if args.end else None
    if start and end and end < start:
        raise ValueError("end must be on or after start")

    report_dir = resolve_report_dir(
        args.report_dir,
        symbols=args.symbol,
        start=start,
        end=end,
    )
    raw_dir = Path(args.raw_dir)
    connection = psycopg2.connect(require_database_url())
    connection.autocommit = False
    try:
        invalid = fetch_invalid_bars(
            connection,
            args.source.strip().upper(),
            symbols=args.symbol,
            start=start,
            end=end,
        )
        details_path, summary_path = write_invalid_reports(invalid, report_dir)
        ranges = group_repair_ranges(
            invalid,
            max_gap_days=args.max_gap_days,
            max_range_days=args.max_range_days,
        )
        print(f"invalid_rows={len(invalid)}")
        print(f"repair_ranges={len(ranges)}")
        print(f"details_report={details_path.resolve()}")
        print(f"summary_report={summary_path.resolve()}")

        if not args.fetch:
            connection.rollback()
            return

        selected = select_ranges(
            ranges,
            symbols=args.symbol,
            allow_all=args.all,
            limit_symbols=args.limit_symbols,
        )
        client = KISClient.from_env()
        stats, result_path = run_repair(
            connection,
            client,
            selected,
            args,
            report_dir,
            raw_dir,
        )
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    print(f"mode={'APPLY' if args.apply else 'FETCH_ONLY'}")
    print(f"selected_ranges={len(selected)}")
    for key in sorted(stats):
        print(f"{key.lower()}={stats[key]}")
    print(f"repair_report={result_path.resolve()}")


if __name__ == "__main__":
    main()
