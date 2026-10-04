"""공급자 수정주가를 검토 전 일반 OHLC와 섞지 않는 저장 경계."""

from backfills.price_bar_invalid_repair import upsert_price_bars
from psycopg2.extras import execute_values


def require_adjusted(value):
    if type(value) is not bool:
        raise ValueError("수정주가 여부는 명시적인 boolean이어야 합니다.")
    return value


def persist_price_rows(
    connection, rows, source, interval, currency, source_object_id, return_records=True
):
    if not rows:
        return [] if return_records else 0
    flags = {require_adjusted(row.get("provider_adjusted")) for row in rows}
    if len(flags) != 1:
        raise ValueError("일반 가격과 수정주가 후보를 함께 저장할 수 없습니다.")
    if flags == {False}:
        return upsert_price_bars(
            connection, rows, source, interval, currency, return_records=return_records
        )
    if not source_object_id:
        raise ValueError("수정주가 후보에는 원문 식별자가 필요합니다.")
    # 수집 원문별로 보존한다. 서로 다른 조정 기준일의 응답을 덮어쓰지 않는다.
    values = [
        (
            source_object_id,
            row["asset_id"],
            source,
            interval,
            row["trade_date"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
            currency,
            row["collected_at"],
            row["content_hash"],
        )
        for row in rows
    ]
    with connection.cursor() as cursor:
        records = execute_values(
            cursor,
            """
            INSERT INTO price_adjustment_candidate (
                source_object_id, asset_id, source, "interval", trade_date,
                open, high, low, close, volume, currency, collected_at, content_hash
            ) VALUES %s
            ON CONFLICT (source_object_id, asset_id, source, "interval", trade_date)
            DO UPDATE SET content_hash = price_adjustment_candidate.content_hash
            WHERE price_adjustment_candidate.content_hash = EXCLUDED.content_hash
            RETURNING id, asset_id, source, "interval", trade_date, content_hash
        """,
            values,
            page_size=500,
            fetch=True,
        )
    if len(records) != len(rows):
        raise ValueError(
            "동일 원문의 수정주가 변환 결과가 달라졌습니다. 트랜잭션을 취소하세요."
        )
    if not return_records:
        return len(rows)
    return [
        dict(
            zip(
                ("id", "asset_id", "source", "interval", "bar_time", "content_hash"),
                record,
            ),
            target_table="price_adjustment_candidate",
        )
        for record in records
    ]
