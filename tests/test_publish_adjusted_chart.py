import unittest
from datetime import date
from decimal import Decimal
from backfills.publish_adjusted_chart import validate_series


class PublishAdjustedChartTest(unittest.TestCase):
    def setUp(self):
        self.day=date(2024,6,7)
        self.bar={"date":self.day,"open":Decimal(120),"high":Decimal(121),"low":Decimal(119),"close":Decimal(120),"volume":Decimal(10)}

    def test_all_three_date_sets_must_match(self):
        self.assertEqual(validate_series([self.bar],[self.bar],[self.day])["matchingDates"],1)
        with self.assertRaises(ValueError): validate_series([self.bar],[self.bar],[self.day,date(2024,6,10)])
        with self.assertRaises(ValueError): validate_series([self.bar],[],[self.day])
        with self.assertRaises(ValueError): validate_series([],[],[])

    def test_broken_ohlc_and_negative_volume_are_rejected(self):
        broken={**self.bar,"low":Decimal(121)}
        with self.assertRaises(ValueError): validate_series([self.bar],[broken],[self.day])
        with self.assertRaises(ValueError): validate_series([self.bar],[{**self.bar,"volume":Decimal(-1)}],[self.day])
