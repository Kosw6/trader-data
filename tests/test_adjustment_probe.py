import unittest
from datetime import date
from decimal import Decimal

from backfills.price_adjustment_probe import compare


class AdjustmentProbeTest(unittest.TestCase):
    def test_preserves_non_split_only_difference(self):
        day = date(2024, 6, 7)
        rows = compare(
            [{"date": day, "close": Decimal("1208.88")}],
            [{"date": day, "close": Decimal("120.5508")}],
        )
        self.assertNotEqual(Decimal(rows[0]["ratio"]), Decimal(10))

    def test_missing_side_is_not_zero_or_success(self):
        rows = compare([{"date": date(2024, 6, 7), "close": Decimal(10)}], [])
        self.assertIsNone(rows[0]["adjustedRequestClose"])
        self.assertIsNone(rows[0]["ratio"])

    def test_zero_adjusted_is_not_divided(self):
        day = date(2024, 6, 7)
        rows = compare(
            [{"date": day, "close": Decimal(10)}], [{"date": day, "close": Decimal(0)}]
        )
        self.assertIsNone(rows[0]["ratio"])
