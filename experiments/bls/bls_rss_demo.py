from datetime import datetime, timezone
from pprint import pprint

from experiments.bls.bls_rss import collect_release_sync_results


schedules = [
    {
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
]

published_releases = [
    {
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
]


if __name__ == "__main__":
    results = collect_release_sync_results(
        schedules=schedules,
        releases=published_releases,
    )

    print("\n=== MATCHED ===")
    pprint(results["matched"], sort_dicts=False)

    print("\n=== UNMATCHED ===")
    pprint(results["unmatched"], sort_dicts=False)
