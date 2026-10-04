"""외부 API 없이 저장 경계·원문 계보·NULL 덮어쓰기 방지를 검증한다."""

import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from backfills.price_bar_invalid_repair import run_repair, upsert_price_bars
from workers.kis_price_etl_worker import etl_options
from workers.kis_price_worker import (
    apply_incremental_window,
    build_price_rows,
    upsert_price_bar_lineage,
)
from workers.price_persistence import persist_price_rows, require_adjusted


def price_row(adjusted=False):
    return {
        "asset_id": 13,
        "trade_date": date(2025, 7, 28),
        "open": Decimal(10),
        "high": Decimal(12),
        "low": Decimal(9),
        "close": Decimal(11),
        "volume": 100,
        "provider_adjusted": adjusted,
        "collected_at": datetime(2026, 10, 4, tzinfo=timezone.utc),
        "content_hash": "a" * 64,
    }


class PricePersistenceTest(unittest.TestCase):
    def test_adjusted_collection_does_not_skip_using_raw_latest_date(self):
        connection = MagicMock()
        options = {"adjusted": True, "collection_mode": "INCREMENTAL"}
        self.assertEqual(
            apply_incremental_window(connection, {"asset_id": 13}, options), options
        )
        connection.cursor.assert_not_called()

    def test_adjusted_flag_is_explicit_boolean(self):
        for value in (None, "false", "true", 0, 1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                require_adjusted(value)
        self.assertIs(require_adjusted(False), False)

    def test_etl_rejects_ambiguous_raw_flag(self):
        args = SimpleNamespace(source="KIS", interval="1D", currency="USD")
        for payload in ({}, {"adjusted": "false"}):
            with self.assertRaises(ValueError):
                etl_options({}, payload, args)

    def test_hash_distinguishes_adjustment_mode(self):
        row = price_row()
        bar = dict(row, date=row["trade_date"])
        options = {
            "source": "KIS",
            "interval": "1D",
            "currency": "USD",
            "adjusted": False,
        }
        raw, _ = build_price_rows({"asset_id": 13}, [bar], options, row["collected_at"])
        adjusted, _ = build_price_rows(
            {"asset_id": 13}, [bar], dict(options, adjusted=True), row["collected_at"]
        )
        self.assertNotEqual(raw[0]["content_hash"], adjusted[0]["content_hash"])
        self.assertTrue(adjusted[0]["provider_adjusted"])

    def test_raw_upsert_does_not_clear_adjusted_close(self):
        with patch("backfills.price_bar_invalid_repair.execute_values") as execute:
            upsert_price_bars(MagicMock(), [price_row()], "KIS", "1D", "USD")
        query = execute.call_args_list[0].args[1]
        self.assertNotIn("adjusted_close =", query)

    def test_raw_boundary_rejects_adjusted_rows(self):
        with self.assertRaises(ValueError):
            upsert_price_bars(MagicMock(), [price_row(True)], "KIS", "1D", "USD")

    def test_candidate_preserves_date_and_lineage_target(self):
        row = price_row(True)
        record = (1, 13, "KIS", "1D", row["trade_date"], row["content_hash"])
        with patch(
            "workers.price_persistence.execute_values", return_value=[record]
        ) as execute:
            result = persist_price_rows(MagicMock(), [row], "KIS", "1D", "USD", 42)
        self.assertIn(
            "INSERT INTO price_adjustment_candidate", execute.call_args.args[1]
        )
        self.assertEqual(
            execute.call_args.args[2][0][:5], (42, 13, "KIS", "1D", date(2025, 7, 28))
        )
        with patch("psycopg2.extras.execute_values") as lineage:
            upsert_price_bar_lineage(MagicMock(), 42, result, row["collected_at"])
        self.assertEqual(lineage.call_args.args[2][0][1], "price_adjustment_candidate")

    def test_candidate_requires_provenance_and_uniform_mode(self):
        for rows, source_id in (
            ([price_row(True)], None),
            ([price_row(), price_row(True)], 42),
        ):
            with self.assertRaises(ValueError):
                persist_price_rows(MagicMock(), rows, "KIS", "1D", "USD", source_id)

    def test_changed_transform_for_same_raw_is_rejected(self):
        with (
            patch("workers.price_persistence.execute_values", return_value=[]),
            self.assertRaises(ValueError),
        ):
            persist_price_rows(MagicMock(), [price_row(True)], "KIS", "1D", "USD", 42)

    def test_adjusted_repair_cannot_overwrite_raw_even_before_fetch(self):
        client = MagicMock()
        with self.assertRaises(ValueError):
            run_repair(
                MagicMock(),
                client,
                [],
                SimpleNamespace(apply=True, adjusted=True),
                None,
                None,
            )
        client.get_daily_prices.assert_not_called()

    def test_unadjusted_routes_to_existing_storage(self):
        with patch(
            "workers.price_persistence.upsert_price_bars", return_value=[]
        ) as raw:
            persist_price_rows(MagicMock(), [price_row()], "KIS", "1D", "USD", 42)
        raw.assert_called_once()
