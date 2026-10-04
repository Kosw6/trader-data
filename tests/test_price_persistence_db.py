"""명시적 로컬 TEST_PRICE_DSN에서만 수행하며 모든 가격/후보 변경을 롤백한다."""

import os
import unittest
from datetime import datetime, timezone
from decimal import Decimal

import psycopg2
from workers.kis_price_worker import upsert_price_bar_lineage
from workers.price_persistence import persist_price_rows


@unittest.skipUnless(os.getenv("TEST_PRICE_DSN"), "로컬 DB 통합 테스트 명시 실행 필요")
class PricePersistenceDatabaseTest(unittest.TestCase):
    def test_preservation_candidate_idempotency_lineage_and_rollback(self):
        connection = psycopg2.connect(os.environ["TEST_PRICE_DSN"])
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout = '3s'")
                cursor.execute("SET LOCAL statement_timeout = '10s'")
                cursor.execute("SELECT count(*) FROM price_adjustment_candidate")
                before = cursor.fetchone()[0]
                cursor.execute(
                    "SELECT id FROM source_object WHERE provider='KIS' LIMIT 1"
                )
                source_id = cursor.fetchone()[0]
                cursor.execute("""SELECT asset_id, source, "interval", bar_time,
                    open, high, low, close, volume, currency
                    FROM price_bar WHERE source='KIS' AND quality_status='VALID'
                    AND "interval"='1D' LIMIT 1 FOR UPDATE""")
                (
                    asset,
                    source,
                    interval,
                    stamp,
                    op,
                    high,
                    low,
                    close,
                    volume,
                    currency,
                ) = cursor.fetchone()
                cursor.execute(
                    """UPDATE price_bar SET adjusted_close=123.45
                    WHERE asset_id=%s AND source=%s AND "interval"=%s AND bar_time=%s""",
                    (asset, source, interval, stamp),
                )
            row = {
                "asset_id": asset,
                "trade_date": stamp.astimezone(timezone.utc).date(),
                "open": op,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "collected_at": datetime.now(timezone.utc),
                "content_hash": "b" * 64,
                "provider_adjusted": False,
            }
            persist_price_rows(connection, [row], source, interval, currency, source_id)
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT adjusted_close FROM price_bar
                    WHERE asset_id=%s AND source=%s AND "interval"=%s AND bar_time=%s""",
                    (asset, source, interval, stamp),
                )
                self.assertEqual(cursor.fetchone()[0], Decimal("123.45"))
            row["provider_adjusted"] = True
            first = persist_price_rows(
                connection, [row], source, interval, currency, source_id
            )
            again = persist_price_rows(
                connection, [row], source, interval, currency, source_id
            )
            self.assertEqual(first, again)
            with connection.cursor() as cursor:
                upsert_price_bar_lineage(cursor, source_id, first, row["collected_at"])
                cursor.execute(
                    "SELECT trade_date, basis FROM price_adjustment_candidate WHERE id=%s",
                    (first[0]["id"],),
                )
                self.assertEqual(
                    cursor.fetchone(),
                    (row["trade_date"], "PROVIDER_ADJUSTED_UNREVIEWED"),
                )
                cursor.execute(
                    "SELECT target_table FROM record_lineage WHERE source_object_id=%s AND target_record_id=%s AND target_table='price_adjustment_candidate'",
                    (source_id, first[0]["id"]),
                )
                self.assertEqual(cursor.fetchone()[0], "price_adjustment_candidate")
            with self.assertRaises(ValueError):
                persist_price_rows(
                    connection,
                    [dict(row, content_hash="c" * 64)],
                    source,
                    interval,
                    currency,
                    source_id,
                )
            connection.rollback()
            with connection.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM price_adjustment_candidate")
                self.assertEqual(cursor.fetchone()[0], before)
        finally:
            connection.rollback()
            connection.close()
