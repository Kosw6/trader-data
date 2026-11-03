"""실제 DB/API 없이 필수 거래일과 제외 가능한 추가 날짜의 게시 계약을 확인한다."""

import copy
import json
import os
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

from backfills import backfill_adjusted_charts as collector
from backfills.backfill_adjusted_charts import close_only_dates
from backfills.chart_snapshot_selection import select_snapshot_bars
from backfills.chart_snapshot_store import publish_snapshot

A, B, C = (date(2025, 1, n) for n in (6, 7, 8))


def bar(day, invalid=False):
    return {
        "date": day,
        "open": Decimal(12 if invalid else 10),
        "high": Decimal(11),
        "low": Decimal(9),
        "close": Decimal(10),
        "volume": 1,
    }


class ChartSnapshotSelectionTest(unittest.TestCase):
    def select(self, values, required=(A, C), close_only=(B,)):
        return select_snapshot_bars(values, required, A, C, close_only)

    def test_known_close_only_error_does_not_block_required_dates(self):
        selected, review = self.select([bar(A), bar(B, True), bar(C)])
        self.assertEqual([b["date"] for b in selected], [A, C])
        self.assertEqual(
            (review["receivedRows"], review["rows"], review["excludedRows"]), (3, 2, 1)
        )
        self.assertEqual(
            review["excludedCloseOnlyDates"][0]["ohlcvReasons"], ["HIGH_BELOW_PRICE"]
        )

    def test_known_close_only_is_not_promoted_even_if_adjusted_ohlc_valid(self):
        selected, review = self.select([bar(A), bar(B), bar(C)])
        self.assertEqual(len(selected), 2)
        self.assertEqual(review["excludedCloseOnlyDates"][0]["ohlcvReasons"], [])

    def test_unknown_additional_valid_date_still_allowed(self):
        selected, review = self.select([bar(A), bar(B), bar(C)], close_only=())
        self.assertEqual(len(selected), 3)
        self.assertEqual(review["additionalProviderDates"], 1)

    def test_unknown_additional_invalid_date_rejected(self):
        with self.assertRaises(ValueError):
            self.select([bar(A), bar(B, True), bar(C)], close_only=())

    def test_required_invalid_date_rejected(self):
        with self.assertRaises(ValueError):
            self.select([bar(A, True), bar(B, True), bar(C)])

    def test_required_policy_overlap_rejected(self):
        with self.assertRaises(ValueError):
            self.select([bar(A), bar(C)], close_only=(A,))

    def test_missing_required_interior_date_rejected(self):
        with self.assertRaises(ValueError):
            self.select([bar(A), bar(C)], required=(A, B, C), close_only=())

    def test_missing_endpoint_rejected(self):
        with self.assertRaises(ValueError):
            self.select([bar(A), bar(B, True)])

    def test_duplicate_even_excluded_date_rejected(self):
        with self.assertRaises(ValueError):
            self.select([bar(A), bar(B, True), bar(B, True), bar(C)])

    def test_empty_input_rejected(self):
        with self.assertRaises(ValueError):
            self.select([])

    def test_missing_price_and_negative_volume_required_rejected(self):
        for field, value in (("open", None), ("volume", -1)):
            bad = dict(bar(A), **{field: value})
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.select([bad, bar(C)])

    def test_original_bars_unchanged_and_no_price_fabrication(self):
        values = [bar(C), bar(B, True), bar(A)]
        original = copy.deepcopy(values)
        selected, _ = self.select(values)
        self.assertEqual(values, original)
        self.assertEqual(selected, [original[2], original[0]])

    def test_query_is_scoped_and_parameterized(self):
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value.fetchall.return_value = [(B,)]
        self.assertEqual(close_only_dates(conn, 123, A, C), [B])
        query, params = (
            conn.cursor.return_value.__enter__.return_value.execute.call_args.args
        )
        self.assertEqual(params, (123, A, C))
        for guard in ("asset_id=%s", "source='KIS'", "'1D'", "'CLOSE_ONLY'"):
            self.assertIn(guard, query)

    def test_publisher_still_rejects_unfiltered_invalid_bar_before_db_write(self):
        conn = MagicMock()
        with self.assertRaises(ValueError):
            publish_snapshot(
                conn,
                {"asset_id": 1, "currency": "USD"},
                {"provider": "KIS", "adjusted": True},
                [bar(B, True)],
                B,
                B,
                datetime.now(timezone.utc),
                None,
            )
        conn.cursor.assert_not_called()


class CollectorSelectionIntegrationTest(unittest.TestCase):
    def run_collector(self, directory, policies):
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value.fetchall.return_value = [(1,)]
        asset = {
            "asset_id": 1,
            "currency": "USD",
            "provider_exchange_code": "AMS",
            "provider_symbol": "TEST",
        }
        client = MagicMock()
        client.get_daily_price_page.return_value = {
            "raw_payload": {
                "output2": [
                    {
                        "xymd": d.strftime("%Y%m%d"),
                        "open": "12" if d == B else "10",
                        "high": "11",
                        "low": "9",
                        "clos": "10",
                        "tvol": "1",
                    }
                    for d in (C, B, A)
                ]
            }
        }
        with (
            patch.dict(
                os.environ, {"LOCAL_PRICE_DATABASE_URL": "postgresql://localhost/test"}
            ),
            patch.object(collector.psycopg2, "connect") as connect,
            patch.object(collector, "load_dotenv"),
            patch.object(collector, "load_asset", return_value=asset),
            patch.object(collector, "raw_dates", return_value=[A, C]),
            patch.object(collector, "close_only_dates", side_effect=policies),
            patch.object(collector, "publish_snapshot", return_value=77) as publish,
        ):
            connect.return_value.__enter__.return_value = conn
            try:
                collector.main(
                    [directory, "--apply", "--symbols", "TEST"], shared_client=client
                )
            except SystemExit as error:
                self.assertEqual(error.code, 1)
        state = json.loads((Path(directory) / "state.json").read_text(encoding="utf-8"))
        return publish, conn, state["assets"]["TEST"]

    def test_only_selected_prices_published_but_full_original_payload_preserved(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2] / "tmp") as directory:
            publish, _, entry = self.run_collector(directory, [[B], [B]])
            self.assertEqual(entry["status"], "PUBLISHED")
            args = publish.call_args.args
            self.assertEqual([b["date"] for b in args[3]], [A, C])
            self.assertEqual(len(args[2]["pages"][0]["payload"]["output2"]), 3)
            self.assertEqual(entry["validation"]["excludedRows"], 1)
            self.assertTrue((Path(directory) / "TEST/selection-review.json").exists())

    def test_policy_changed_during_collection_prevents_publication(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2] / "tmp") as directory:
            publish, conn, entry = self.run_collector(directory, [[B], []])
            self.assertEqual(entry["status"], "FAILED")
            publish.assert_not_called()
            conn.rollback.assert_called()

    def test_unknown_invalid_date_is_not_silently_dropped(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[2] / "tmp") as directory:
            publish, _, entry = self.run_collector(directory, [[]])
            self.assertEqual(entry["failureReason"], "OHLCV_INVALID")
            publish.assert_not_called()
