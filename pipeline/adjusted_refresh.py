"""Full-generation price refresh validation; never splice adjustment bases."""

from datetime import date, timedelta
from decimal import Decimal

from collectors.kis_client import normalize_daily_price


def sessions(start, end, holidays):
    days = []
    day = start
    while day <= end:
        if day.weekday() < 5 and str(day) not in holidays:
            days.append(day)
        day += timedelta(days=1)
    return days


def decode_page(payload, symbol, exchange, cursor):
    if str(payload.get("rt_cd")) != "0":
        raise ValueError("PROVIDER_BUSINESS_FAILURE")
    raw = payload.get("output2")
    if not isinstance(raw, list):
        raise TypeError("PROVIDER_SCHEMA_CONFLICT")
    if not raw:
        return []
    if payload.get("output1", {}).get("rsym") != f"D{exchange}{symbol}":
        raise ValueError("PROVIDER_IDENTITY_CONFLICT")
    bars = [normalize_daily_price(row) for row in raw]
    days = [b["date"] for b in bars]
    if len(days) != len(set(days)) or any(day > cursor for day in days):
        raise ValueError("PROVIDER_DATE_CONFLICT")
    return bars


def validate_generation(pages, target, end, holidays, close_only=()):
    start = date.fromisoformat(target["coverage_start"])
    cursor = end
    rows = {}
    for page in pages:
        if (
            page["symbol"] != target["provider_symbol"]
            or page["exchange"] != target["provider_exchange_code"]
            or page["adjusted"] is not True
            or page["cursor"] != str(cursor)
        ):
            raise ValueError("PAGE_PLAN_CONFLICT")
        bars = decode_page(page["payload"], page["symbol"], page["exchange"], cursor)
        if not bars:
            raise ValueError("EMPTY_BEFORE_HISTORY_COMPLETE")
        for bar in bars:
            if start <= bar["date"] <= end:
                if bar["date"] in rows:
                    raise ValueError("DUPLICATE_GENERATION_DATE")
                rows[bar["date"]] = bar
        oldest = min(b["date"] for b in bars)
        cursor = oldest - timedelta(days=1)
    if not rows or min(rows) != start or max(rows) != end or cursor >= start:
        raise ValueError("INCOMPLETE_GENERATION")
    # Older sessions are checked against the previous complete snapshot at publication.
    new_start = date.fromisoformat(target["coverage_end"]) + timedelta(days=1)
    required = set(sessions(new_start, end, holidays))
    actual_new = {d for d in rows if d >= new_start}
    if actual_new != required:
        raise ValueError("NEW_SESSION_COVERAGE_CONFLICT")
    excluded = set(close_only)
    result = []
    for day, bar in sorted(rows.items()):
        if str(day) in excluded:
            continue
        values = [bar[k] for k in ("open", "high", "low", "close")]
        if any(v is None or not v.is_finite() or v <= 0 for v in values):
            raise ValueError("INVALID_OHLC")
        if bar["low"] > min(values) or bar["high"] < max(values):
            raise ValueError("INVALID_OHLC_RANGE")
        volume = bar["volume"]
        if volume is not None and (not volume.is_finite() or volume < Decimal(0)):
            raise ValueError("INVALID_VOLUME")
        result.append(
            {"date": str(day)}
            | {
                k: str(bar[k]) if bar[k] is not None else None
                for k in ("open", "high", "low", "close", "volume")
            }
        )
    if not result or result[0]["date"] != str(start) or result[-1]["date"] != str(end):
        raise ValueError("EXCLUDED_ENDPOINT")
    return result
