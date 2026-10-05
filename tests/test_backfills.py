import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from backfills.asset_company_match_backfill import build_match_candidates
from archive.legacy_stock.price_bar_backfill import iter_time_windows, parse_timestamp
from backfills.price_bar_invalid_repair import (
    InvalidBar,
    build_repair_rows,
    group_repair_ranges,
    invalid_reasons,
    resolve_report_dir,
)

from backfills.sec_company_backfill import (
    company_aliases,
    match_sec_ciks_to_kis_assets,
    normalize_cik,
    normalize_alias_period,
    normalize_sec_exchange,
)
from backfills.stock_info_backfill import (
    normalize_exchange,
    resolve_asset_type,
    transform_stock_info,
)


class StockInfoBackfillTest(unittest.TestCase):
    """레거시 KIS 종목 마스터가 신규 Asset 계약으로 변환되는지 검증한다."""

    def test_transforms_equity_and_provider_identity(self):
        """NASDAQ 주식의 표준 거래소와 한글·영문 Alias를 생성하는지 확인한다."""
        record = transform_stock_info(
            (
                11,
                "NAS",
                "AAPL",
                "애플",
                "APPLE INC",
                2,
                730,
                0,
            )
        )
        self.assertEqual("NASDAQ", record["exchange"])
        self.assertEqual("EQUITY", record["asset_type"])
        self.assertEqual("NAS", record["provider_exchange_code"])
        self.assertEqual("AAPL", record["ticker"])
        self.assertEqual(2, len(record["aliases"]))
        self.assertEqual(
            {"KOREAN_NAME", "ENGLISH_NAME"},
            {alias["alias_type"] for alias in record["aliases"]},
        )

    def test_maps_etp_subtypes_without_treating_them_as_companies(self):
        """KIS classification_code에 따라 ETF·ETN 유형을 구분하는지 확인한다."""
        self.assertEqual("ETF", resolve_asset_type(3, 1))
        self.assertEqual("ETN", resolve_asset_type(3, 2))
        self.assertEqual("VIX_ETF", resolve_asset_type(3, 5))
        self.assertEqual("NYSE_AMERICAN", normalize_exchange("AMS"))

    def test_rejects_unknown_exchange(self):
        """정의하지 않은 제공자 거래소를 임의로 적재하지 않는지 확인한다."""
        with self.assertRaises(ValueError):
            normalize_exchange("UNKNOWN")


class SECCompanyBackfillTest(unittest.TestCase):
    """SEC Company 백필의 CIK와 법인명 Alias 변환 규칙을 검증한다."""

    def test_normalizes_cik(self):
        """숫자 CIK를 앞자리 0이 보존된 10자리 문자열로 변환하는지 확인한다."""
        self.assertEqual("0000320193", normalize_cik(320193))

    def test_builds_current_and_former_company_aliases(self):
        """현재 법인명과 과거 법인명의 유형·기간을 구분하는지 확인한다."""
        aliases = company_aliases(
            {
                "name": "APPLE INC",
                "formerNames": [
                    {
                        "name": "APPLE COMPUTER INC",
                        "from": "1977-01-03",
                        "to": "2007-01-09",
                    }
                ],
            }
        )
        self.assertEqual(2, len(aliases))
        self.assertEqual("LEGAL_NAME", aliases[0]["alias_type"])
        self.assertEqual("FORMER_NAME", aliases[1]["alias_type"])
        self.assertEqual("2007-01-09", aliases[1]["valid_to"].isoformat())

    def test_orders_reversed_sec_former_name_period(self):
        """SEC source anomalies must not violate the company_alias period check."""
        valid_from, valid_to, corrected = normalize_alias_period(
            "1995-02-16T00:00:00.000Z",
            "1995-01-27T00:00:00.000Z",
        )
        self.assertTrue(corrected)
        self.assertEqual("1995-01-27", valid_from.isoformat())
        self.assertEqual("1995-02-16", valid_to.isoformat())

    def test_company_aliases_marks_reversed_period_as_corrected(self):
        aliases = company_aliases(
            {
                "cik": "0000771497",
                "name": "ABM INDUSTRIES INC /DE/",
                "formerNames": [
                    {
                        "name": "ABM INDUSTRIES INC",
                        "from": "1995-02-16T00:00:00.000Z",
                        "to": "1995-01-27T00:00:00.000Z",
                    }
                ],
            }
        )
        self.assertTrue(aliases[1]["period_corrected"])
        self.assertLessEqual(aliases[1]["valid_from"], aliases[1]["valid_to"])

    def test_matches_sec_and_kis_by_exact_ticker_and_exchange(self):
        """SEC와 KIS의 ticker·표준 거래소가 모두 같은 CIK만 선택하는지 확인한다."""
        payload = {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                [320193, "APPLE INC", "AAPL", "Nasdaq"],
                [1045810, "NVIDIA CORP", "NVDA", "Nasdaq"],
                [999999, "WRONG EXCHANGE", "AAPL", "NYSE"],
                [888888, "UNSUPPORTED", "TEST", "OTC"],
            ],
        }
        matched, stats = match_sec_ciks_to_kis_assets(
            payload,
            {("AAPL", "NASDAQ"), ("NVDA", "NASDAQ")},
        )
        self.assertEqual({"0000320193", "0001045810"}, matched)
        self.assertEqual(2, stats["matched_rows"])
        self.assertEqual(1, stats["unsupported_or_empty"])
        self.assertEqual("NYSE_AMERICAN", normalize_sec_exchange("NYSE American"))


class AssetCompanyMatchBackfillTest(unittest.TestCase):
    """Verify exact and ambiguous SEC-KIS candidate classification."""

    def setUp(self):
        self.assets = [
            {
                "asset_id": 13,
                "ticker": "AAPL",
                "asset_name": "Apple",
                "exchange": "NASDAQ",
                "asset_type": "EQUITY",
                "current_company_id": None,
                "provider_exchange_code": "NAS",
                "provider_symbol": "AAPL",
            }
        ]
        self.companies = {
            "0000320193": {
                "company_id": 1,
                "cik": "0000320193",
                "company_name": "APPLE INC",
                "sec_source_object_id": 10,
            }
        }

    def test_builds_unique_candidate_without_implicit_confirmation(self):
        payload = {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [[320193, "APPLE INC", "AAPL", "Nasdaq"]],
        }
        candidates, stats = build_match_candidates(
            payload, self.assets, self.companies
        )
        self.assertEqual(1, len(candidates))
        self.assertEqual("CANDIDATE", candidates[0]["status"])
        self.assertEqual(1.0, candidates[0]["match_confidence"])
        self.assertEqual(1, stats["unique_exact"])

    def test_auto_confirms_only_unique_exact_candidate(self):
        payload = {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [[320193, "APPLE INC", "AAPL", "Nasdaq"]],
        }
        candidates, _ = build_match_candidates(
            payload, self.assets, self.companies, auto_confirm_exact=True
        )
        self.assertEqual("CONFIRMED", candidates[0]["status"])

    def test_marks_multiple_companies_for_review(self):
        companies = {
            **self.companies,
            "0000000002": {
                "company_id": 2,
                "cik": "0000000002",
                "company_name": "OTHER APPLE",
                "sec_source_object_id": 11,
            },
        }
        payload = {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                [320193, "APPLE INC", "AAPL", "Nasdaq"],
                [2, "OTHER APPLE", "AAPL", "Nasdaq"],
            ],
        }
        candidates, stats = build_match_candidates(payload, self.assets, companies)
        self.assertEqual(2, len(candidates))
        self.assertEqual({"REVIEW_REQUIRED"}, {item["status"] for item in candidates})
        self.assertEqual(2, stats["review_required"])


class PriceBarBackfillTest(unittest.TestCase):
    """Verify daily-bar time normalization and half-open batch windows."""

    def test_date_only_timestamp_is_utc_midnight(self):
        parsed = parse_timestamp("2024-06-14")
        self.assertEqual(datetime(2024, 6, 14, tzinfo=timezone.utc), parsed)

    def test_builds_adjacent_windows_without_overlap(self):
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 7, 1, tzinfo=timezone.utc)
        windows = list(iter_time_windows(start, end, 90))
        self.assertEqual(3, len(windows))
        self.assertEqual(start, windows[0][0])
        self.assertEqual(windows[0][1], windows[1][0])
        self.assertEqual(end, windows[-1][1])


class PriceBarInvalidRepairTest(unittest.TestCase):
    """Verify invalid classification, request grouping, and safe repair selection."""

    def build_invalid(self, trade_date, symbol="XLF"):
        """Create a compact invalid legacy row used by grouping tests."""
        return InvalidBar(
            asset_id=1,
            exchange_code="NYS",
            symbol=symbol,
            trade_date=trade_date,
            open=Decimal("31.94"),
            high=Decimal("32.40"),
            low=Decimal("31.51"),
            close=Decimal("33.15"),
            volume=Decimal("100"),
            reasons=("HIGH_BELOW_PRICE",),
        )

    def test_classifies_high_and_low_violations(self):
        self.assertEqual(
            ("HIGH_BELOW_PRICE",),
            invalid_reasons(
                Decimal("10"),
                Decimal("11"),
                Decimal("9"),
                Decimal("12"),
                Decimal("100"),
            ),
        )
        self.assertEqual(
            ("LOW_ABOVE_PRICE",),
            invalid_reasons(
                Decimal("10"),
                Decimal("12"),
                Decimal("10"),
                Decimal("9"),
                Decimal("100"),
            ),
        )

    def test_groups_nearby_dates_and_splits_large_gaps(self):
        items = [
            self.build_invalid(date(2024, 1, 1)),
            self.build_invalid(date(2024, 1, 20)),
            self.build_invalid(date(2024, 4, 1)),
        ]
        ranges = group_repair_ranges(items, max_gap_days=30, max_range_days=366)
        self.assertEqual(2, len(ranges))
        self.assertEqual(2, len(ranges[0].items))
        self.assertEqual(date(2024, 4, 1), ranges[1].start_date)

    def test_builds_rows_only_for_exact_valid_dates(self):
        original = self.build_invalid(date(2024, 1, 2))
        repair_range = group_repair_ranges([original])[0]
        fetched = [
            {
                "date": date(2024, 1, 2),
                "open": Decimal("10"),
                "high": Decimal("12"),
                "low": Decimal("9"),
                "close": Decimal("11"),
                "volume": Decimal("100"),
            },
            {
                "date": date(2024, 1, 3),
                "open": Decimal("20"),
                "high": Decimal("22"),
                "low": Decimal("19"),
                "close": Decimal("21"),
                "volume": Decimal("200"),
            },
        ]
        rows, statuses = build_repair_rows(
            repair_range,
            fetched,
            datetime(2026, 6, 22, tzinfo=timezone.utc),
        )
        self.assertEqual(1, len(rows))
        self.assertEqual(date(2024, 1, 2), rows[0]["trade_date"])
        self.assertEqual("RECOVERABLE", statuses[0][1])

    def test_filtered_report_uses_scoped_directory(self):
        """표본 실행 결과가 기존 전체 invalid 보고서를 덮어쓰지 않는지 확인한다."""
        path = resolve_report_dir(
            "reports",
            symbols=["aa"],
            start=date(2021, 5, 5),
            end=date(2021, 5, 5),
        )
        self.assertEqual(
            Path("reports") / "AA_2021-05-05_2021-05-05",
            path,
        )


if __name__ == "__main__":
    unittest.main()
