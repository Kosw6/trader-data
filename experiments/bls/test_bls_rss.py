import unittest
from datetime import datetime, timezone

from experiments.bls.bls_rss import collect_release_sync_results, parse_rss_entry


CPI_SCHEDULE_FIXTURE = {
    "provider": "BLS",
    "external_id": "8ebeaa71-d848-4dac-868b-3b08578d899c",
    "release_type": "CPI",
    "title": "Consumer Price Index",
    "frequency": None,
    "scheduled_at": datetime(2026, 4, 10, 12, 30, tzinfo=timezone.utc),
    "actual_released_at": None,
    "reference_period": None,
    "sequence_number": 1,
    "status": "SCHEDULED",
    "source_url": None,
    "source_updated_at": None,
    "collected_at": datetime(2026, 6, 18, 4, 24, 24, tzinfo=timezone.utc),
    "raw_payload": "BEGIN:VEVENT\r\nSUMMARY:Consumer Price Index\r\nEND:VEVENT\r\n",
}

CPI_RELEASE_FIXTURE = {
    "rss_external_id": "test-cpi-release-2026-03",
    "release_type": "CPI",
    "rss_title": "Consumer Price Index - March 2026",
    "reference_period": "2026-03",
    "reference_period_source": "RSS_TITLE",
    "actual_released_at": datetime(
        2026, 4, 10, 12, 30, 54, tzinfo=timezone.utc
    ),
    "source_url": "https://www.bls.gov/news.release/cpi.test.htm",
}


class BlsReleaseSyncTest(unittest.TestCase):
    def test_matches_injected_calendar_and_release_data(self):
        results = collect_release_sync_results(
            schedules=[CPI_SCHEDULE_FIXTURE],
            releases=[CPI_RELEASE_FIXTURE],
        )

        self.assertEqual(1, len(results["matched"]))
        self.assertEqual([], results["unmatched"])

        matched = results["matched"][0]
        self.assertEqual(CPI_SCHEDULE_FIXTURE["external_id"], matched["external_id"])
        self.assertEqual("PUBLISHED", matched["status"])
        self.assertEqual("2026-03", matched["reference_period"])
        self.assertEqual(
            CPI_RELEASE_FIXTURE["actual_released_at"],
            matched["actual_released_at"],
        )

    def test_keeps_release_when_no_calendar_event_matches(self):
        results = collect_release_sync_results(
            schedules=[],
            releases=[CPI_RELEASE_FIXTURE],
        )

        self.assertEqual([], results["matched"])
        self.assertEqual([CPI_RELEASE_FIXTURE], results["unmatched"])

    def test_ignores_latest_numbers_aggregate_item(self):
        entry = {
            "id": "https://www.bls.gov/bls/",
            "title": "Major Economic Indicators Latest Numbers",
            "summary": "CPI data for May 2026",
            "published": "Tue, 16 Jun 2026 12:30:54 GMT",
            "link": "https://www.bls.gov/bls/",
        }

        parsed = parse_rss_entry(entry)

        self.assertEqual("OTHER", parsed["release_type"])
        self.assertIsNone(parsed["reference_period"])


if __name__ == "__main__":
    unittest.main()
