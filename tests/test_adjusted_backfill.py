import unittest
from datetime import date
from backfills.backfill_adjusted_charts import validate_bars, SYMBOLS, symbol_folder


class AdjustedBackfillTest(unittest.TestCase):
    def test_provider_preferred_share_symbol_is_not_a_path(self):
        self.assertEqual(symbol_folder('HFRO/B'),'HFRO%2FB')
        self.assertEqual(symbol_folder('BRK.B'),'BRK.B')
        for symbol in ('../escape','/absolute','A\\B','A:drive',''):
            with self.assertRaises(ValueError): symbol_folder(symbol)

    def test_only_six_approved_targets(self):
        self.assertEqual(set(SYMBOLS),{"NVDA","AAPL","MSFT","AMZN","GOOGL","TSLA"})

    def test_provider_can_fill_missing_local_dates_without_dropping_known_dates(self):
        a,b,c=date(2024,6,3),date(2024,6,4),date(2024,6,5)
        result=validate_bars([{"date":day} for day in (a,b,c)],[a,c],a,c)
        self.assertEqual(result["additionalProviderDates"],1)

    def test_missing_duplicate_or_wrong_boundary_cannot_be_published(self):
        a,b=date(2024,6,3),date(2024,6,4)
        for bars in ([],[{"date":a}],[{"date":a},{"date":a}]):
            with self.assertRaises(ValueError): validate_bars(bars,[a,b],a,b)
