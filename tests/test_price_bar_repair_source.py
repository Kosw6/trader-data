import unittest
from datetime import date
from unittest.mock import MagicMock

from backfills.price_bar_invalid_repair import fetch_invalid_bars


class RepairSourceTest(unittest.TestCase):
    def test_reads_current_prices_with_source_interval_and_date_bounds(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.__iter__.return_value = iter([])
        self.assertEqual([], fetch_invalid_bars(connection, "KIS", [" nvda "], date(2024, 1, 1), date(2024, 1, 2)))
        query, params = cursor.execute.call_args.args
        self.assertIn("FROM price_bar s", query)
        self.assertNotIn("FROM stock", query)
        self.assertIn("aps.asset_id = s.asset_id", query)
        self.assertIn('s."interval" = %s', query)
        self.assertEqual(["KIS", "1D", ["NVDA"]], params[:3])
        self.assertEqual(date(2024, 1, 3), params[-1].date())
