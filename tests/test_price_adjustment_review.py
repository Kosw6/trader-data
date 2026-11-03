import unittest
from datetime import date
from decimal import Decimal
from backfills.price_adjustment_review import review


def bar(close="120", volume="100"):
    return {"date": date(2024,6,7), "open": Decimal(close), "high": Decimal(close)+1,
            "low": Decimal(close)-1, "close": Decimal(close),
            "volume": Decimal(volume) if volume is not None else None}


class PriceAdjustmentReviewTest(unittest.TestCase):
    def test_ohlcv_observation_is_not_approval(self):
        result = review([bar("1200")], [bar("120")])
        self.assertEqual(result["validOhlcPairs"], 1)
        self.assertEqual(result["approval"], "NOT_APPROVED")
        self.assertEqual(result["rows"][0]["volumeComparison"], "SAME")

    def test_missing_volume_is_unknown_not_same(self):
        result = review([bar(volume=None)], [bar(volume=None)])
        self.assertEqual(result["rows"][0]["volumeComparison"], "UNKNOWN")

    def test_missing_date_and_invalid_ohlc_are_visible(self):
        broken = bar(); broken["high"] = Decimal(1)
        self.assertEqual(review([bar()], [broken])["validOhlcPairs"], 0)
        self.assertEqual(review([bar()], [])["matchingDates"], 0)

    def test_duplicate_is_not_silently_overwritten(self):
        with self.assertRaises(ValueError):
            review([bar(), bar()], [bar()])
