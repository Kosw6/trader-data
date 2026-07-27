import unittest
from datetime import date
from decimal import Decimal

from collectors.kis_client import KISAPIError, normalize_daily_price, parse_token_expiry


class KISClientTest(unittest.TestCase):
    """Verify provider response parsing without making real network calls."""

    def test_normalizes_daily_price_fields(self):
        """KIS 문자열 필드를 날짜와 Decimal 기반 일봉으로 변환한다."""
        bar = normalize_daily_price(
            {
                "xymd": "20240614",
                "open": "100.10",
                "high": "102.20",
                "low": "99.90",
                "clos": "101.50",
                "tvol": "123456",
            }
        )
        self.assertEqual(date(2024, 6, 14), bar["date"])
        self.assertEqual(Decimal("101.50"), bar["close"])
        self.assertEqual(Decimal("123456"), bar["volume"])

    def test_rejects_invalid_trade_date(self):
        """잘못된 거래일은 조용히 누락하지 않고 명시적인 API 오류로 처리한다."""
        with self.assertRaises(KISAPIError):
            normalize_daily_price({"xymd": "not-a-date"})

    def test_parses_cached_token_expiry(self):
        """기존 .env 토큰 만료 시각 형식을 읽을 수 있는지 확인한다."""
        parsed = parse_token_expiry("2026-06-22 23:59:59")
        self.assertEqual(2026, parsed.year)
        self.assertEqual(59, parsed.second)


if __name__ == "__main__":
    unittest.main()
