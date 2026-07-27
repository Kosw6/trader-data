import argparse
import logging
import os
from collections import Counter
from datetime import datetime, timedelta, timezone

import psycopg2
from dotenv import load_dotenv


LOGGER = logging.getLogger("price-bar-backfill")
DEFAULT_SOURCE = "KIS"
DEFAULT_INTERVAL = "1D"


def parse_args():
    """Read the source period, batch size, and write-safety options."""
    parser = argparse.ArgumentParser(
        description="Backfill legacy stock daily bars into price_bar."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write price_bar rows. Without this flag only validate and count.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Use the complete min/max timestamp range from the stock hypertable.",
    )
    parser.add_argument("--start", help="Inclusive ISO timestamp or YYYY-MM-DD.")
    parser.add_argument("--end", help="Exclusive ISO timestamp or YYYY-MM-DD.")
    parser.add_argument("--chunk-days", type=int, default=90)
    parser.add_argument(
        "--limit-windows",
        type=int,
        help="Process only the first N time windows for a smoke test.",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        help="Restrict to one ticker. Repeat to select multiple tickers.",
    )
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--interval", default=DEFAULT_INTERVAL)
    parser.add_argument("--currency", default="USD")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def require_database_url():
    """Return the PostgreSQL URL shared with the local Trader database."""
    value = os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError("DATABASE_URL is required in .env or the environment")
    return value


def parse_timestamp(value):
    """Parse an ISO value and treat a date-only value as UTC midnight."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def iter_time_windows(start, end, chunk_days):
    """Yield adjacent half-open time windows suitable for chunk pruning."""
    current = start
    step = timedelta(days=chunk_days)
    while current < end:
        window_end = min(current + step, end)
        yield current, window_end
        current = window_end


def fetch_source_bounds(connection):
    """Read the exact legacy stock timestamp range."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM stock")
        minimum, maximum = cursor.fetchone()
    if minimum is None or maximum is None:
        raise ValueError("legacy stock table is empty")
    return minimum, maximum + timedelta(days=1)


def validate_provider_symbols(connection, provider):
    """Reject ambiguous provider symbols before a symbol-only legacy join."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT provider_symbol, COUNT(*)
            FROM asset_provider_symbol
            WHERE provider = %s
              AND is_current = TRUE
            GROUP BY provider_symbol
            HAVING COUNT(*) > 1
            ORDER BY provider_symbol
            LIMIT 20
            """,
            (provider,),
        )
        duplicates = cursor.fetchall()
    if duplicates:
        sample = ", ".join(symbol for symbol, _count in duplicates)
        raise ValueError(
            "current provider symbols are ambiguous; add exchange mapping first: "
            f"{sample}"
        )


def symbol_filter_sql(symbols, alias="s"):
    """Build an optional ticker predicate and its psycopg2 parameters."""
    if not symbols:
        return "", []
    normalized = sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})
    if not normalized:
        return "", []
    return f" AND {alias}.symb = ANY(%s)", [normalized]


def validation_condition(alias="s"):
    """Return source-bar checks required by non-null and OHLC invariants."""
    return f"""
        {alias}.open IS NOT NULL
        AND {alias}.high IS NOT NULL
        AND {alias}.low IS NOT NULL
        AND {alias}.close IS NOT NULL
        AND {alias}.high >= GREATEST({alias}.open, {alias}.close, {alias}.low)
        AND {alias}.low <= LEAST({alias}.open, {alias}.close, {alias}.high)
        AND ({alias}.volume IS NULL OR {alias}.volume >= 0)
    """


def inspect_window(connection, start, end, provider, symbols):
    """Count eligible, invalid, and unmapped source rows without writing."""
    symbol_sql, symbol_params = symbol_filter_sql(symbols)
    valid_sql = validation_condition()
    query = f"""
        SELECT
            COUNT(*) AS source_rows,
            COUNT(*) FILTER (
                WHERE aps.asset_id IS NOT NULL AND {valid_sql}
            ) AS eligible_rows,
            COUNT(*) FILTER (
                WHERE aps.asset_id IS NULL
            ) AS unmapped_rows,
            COUNT(*) FILTER (
                WHERE aps.asset_id IS NOT NULL AND NOT ({valid_sql})
            ) AS invalid_rows
        FROM stock s
        LEFT JOIN asset_provider_symbol aps
          ON aps.provider = %s
         AND aps.provider_symbol = s.symb
         AND aps.is_current = TRUE
        WHERE s.timestamp >= %s
          AND s.timestamp < %s
          {symbol_sql}
    """
    with connection.cursor() as cursor:
        cursor.execute(query, [provider, start, end, *symbol_params])
        source_rows, eligible_rows, unmapped_rows, invalid_rows = cursor.fetchone()
    return {
        "source_rows": source_rows,
        "eligible_rows": eligible_rows,
        "unmapped_rows": unmapped_rows,
        "invalid_rows": invalid_rows,
    }


def migrate_window(
    connection,
    start,
    end,
    provider,
    interval,
    currency,
    symbols,
    collected_at,
):
    """Upsert one time window entirely inside PostgreSQL and return row count."""
    symbol_sql, symbol_params = symbol_filter_sql(symbols)
    valid_sql = validation_condition()
    query = f"""
        WITH affected AS (
            INSERT INTO price_bar (
                asset_id, source, "interval", bar_time,
                open, high, low, close, adjusted_close, volume,
                currency, collected_at, content_hash,
                quality_status, display_policy, quality_checked_at
            )
            SELECT
                aps.asset_id,
                %s,
                %s,
                s.timestamp,
                s.open,
                s.high,
                s.low,
                s.close,
                NULL,
                s.volume,
                %s,
                %s,
                NULL,
                'VALID',
                'FULL_OHLC',
                %s
            FROM stock s
            JOIN asset_provider_symbol aps
              ON aps.provider = %s
             AND aps.provider_symbol = s.symb
             AND aps.is_current = TRUE
            WHERE s.timestamp >= %s
              AND s.timestamp < %s
              AND {valid_sql}
              {symbol_sql}
            ON CONFLICT (asset_id, source, "interval", bar_time)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                adjusted_close = EXCLUDED.adjusted_close,
                volume = EXCLUDED.volume,
                currency = EXCLUDED.currency,
                collected_at = EXCLUDED.collected_at,
                quality_status = EXCLUDED.quality_status,
                display_policy = EXCLUDED.display_policy,
                quality_checked_at = EXCLUDED.quality_checked_at
            RETURNING 1
        )
        SELECT COUNT(*) FROM affected
    """
    params = [
        provider,
        interval,
        currency,
        collected_at,
        collected_at,
        provider,
        start,
        end,
        *symbol_params,
    ]
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.fetchone()[0]


def resolve_range(connection, args):
    """Resolve either an explicit half-open period or the full source range."""
    if args.all:
        if args.start or args.end:
            raise ValueError("--all cannot be combined with --start or --end")
        return fetch_source_bounds(connection)
    if not args.start or not args.end:
        raise ValueError("specify --all or both --start and --end")
    start = parse_timestamp(args.start)
    end = parse_timestamp(args.end)
    if end <= start:
        raise ValueError("end must be greater than start")
    return start, end


def run_backfill(connection, args):
    """Validate or migrate price history in independently committed windows."""
    provider = args.source.strip().upper()
    interval = args.interval.strip().upper()
    currency = args.currency.strip().upper()
    symbols = args.symbol or []
    validate_provider_symbols(connection, provider)
    start, end = resolve_range(connection, args)
    windows = list(iter_time_windows(start, end, args.chunk_days))
    if args.limit_windows is not None:
        windows = windows[: args.limit_windows]

    stats = Counter()
    collected_at = datetime.now(timezone.utc)
    last_completed_end = None
    for index, (window_start, window_end) in enumerate(windows, start=1):
        inspection = inspect_window(
            connection, window_start, window_end, provider, symbols
        )
        stats.update(inspection)

        if args.apply:
            affected = migrate_window(
                connection,
                window_start,
                window_end,
                provider,
                interval,
                currency,
                symbols,
                collected_at,
            )
            connection.commit()
            stats["affected_rows"] += affected
        else:
            connection.rollback()

        stats["windows"] += 1
        last_completed_end = window_end
        LOGGER.info(
            "window=%d/%d start=%s end=%s source=%d eligible=%d "
            "unmapped=%d invalid=%d",
            index,
            len(windows),
            window_start.isoformat(),
            window_end.isoformat(),
            inspection["source_rows"],
            inspection["eligible_rows"],
            inspection["unmapped_rows"],
            inspection["invalid_rows"],
        )
    return stats, start, end, last_completed_end


def main():
    """Load configuration and execute a dry-run or idempotent price backfill."""
    load_dotenv()
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.chunk_days <= 0:
        raise ValueError("chunk-days must be positive")
    if args.limit_windows is not None and args.limit_windows <= 0:
        raise ValueError("limit-windows must be positive")

    connection = psycopg2.connect(require_database_url())
    connection.autocommit = False
    try:
        stats, start, end, last_completed_end = run_backfill(connection, args)
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    mode = "APPLY" if args.apply else "DRY_RUN"
    print(
        f"mode={mode} start={start.isoformat()} end={end.isoformat()} "
        f"last_completed_end={last_completed_end.isoformat() if last_completed_end else None}"
    )
    for key in sorted(stats):
        print(f"{key}={stats[key]}")
    if stats["unmapped_rows"] or stats["invalid_rows"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
