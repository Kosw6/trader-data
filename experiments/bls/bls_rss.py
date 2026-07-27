import re
from datetime import timezone
from email.utils import parsedate_to_datetime

import feedparser
import requests
from bs4 import BeautifulSoup

from experiments.bls.bls_calender import (
    HEADERS,
    fetch_release_schedule,
    normalize_release_type,
)


BLS_RSS_URL = "https://www.bls.gov/feed/bls_latest.rss"

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def extract_reference_period(text):
    if not text:
        return None

    month_pattern = "|".join(MONTHS)
    month_match = re.search(
        rf"\b({month_pattern})\s+(20\d{{2}})\b",
        text,
        flags=re.IGNORECASE,
    )
    if month_match:
        month = MONTHS[month_match.group(1).lower()]
        return f"{int(month_match.group(2)):04d}-{month:02d}"

    quarter_match = re.search(
        r"\b(?:Q([1-4])\s+(20\d{2})|(20\d{2})\s+Q([1-4]))\b",
        text,
        flags=re.IGNORECASE,
    )
    if quarter_match:
        quarter = quarter_match.group(1) or quarter_match.group(4)
        year = quarter_match.group(2) or quarter_match.group(3)
        return f"{year}-Q{quarter}"

    return None


def fetch_release_page_text(url):
    response = requests.get(url, headers=HEADERS, timeout=15)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    candidates = soup.select("title, h1, h2, main p")
    return " ".join(tag.get_text(" ", strip=True) for tag in candidates[:30])


def parse_published_at(entry):
    published = entry.get("published") or entry.get("updated")
    if not published:
        return None

    value = parsedate_to_datetime(published)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_rss_entry(entry):
    title = entry.get("title", "")
    summary_html = entry.get("summary", "")
    summary = BeautifulSoup(summary_html, "html.parser").get_text(" ", strip=True)
    source_url = entry.get("link")
    release_type = normalize_release_type(title)

    # The latest-numbers feed also contains aggregate pages whose summaries
    # mention several indicators. Only release-specific titles are candidates.
    if release_type == "OTHER":
        return {
            "rss_external_id": entry.get("id") or source_url,
            "release_type": "OTHER",
            "rss_title": title,
            "reference_period": None,
            "reference_period_source": None,
            "actual_released_at": parse_published_at(entry),
            "source_url": source_url,
        }

    reference_period = extract_reference_period(title)
    reference_period_source = "RSS_TITLE" if reference_period else None
    if reference_period is None:
        reference_period = extract_reference_period(summary)
        if reference_period:
            reference_period_source = "RSS_SUMMARY"
    if reference_period is None and source_url:
        try:
            reference_period = extract_reference_period(
                fetch_release_page_text(source_url)
            )
            if reference_period:
                reference_period_source = "RELEASE_PAGE"
        except requests.RequestException as error:
            print(f"BLS release page fetch failed: {source_url}: {error}")

    return {
        "rss_external_id": entry.get("id") or source_url,
        "release_type": release_type,
        "rss_title": title,
        "reference_period": reference_period,
        "reference_period_source": reference_period_source,
        "actual_released_at": parse_published_at(entry),
        "source_url": source_url,
    }


def fetch_published_releases():
    feed = feedparser.parse(BLS_RSS_URL, request_headers=HEADERS)
    if feed.bozo:
        raise RuntimeError(f"BLS RSS parse failed: {feed.bozo_exception}")

    releases = []
    for entry in feed.entries:
        release = parse_rss_entry(entry)
        if release["release_type"] != "OTHER":
            releases.append(release)
    return releases


def merge_schedule_and_release(schedules, release, max_gap_hours=24):
    published_at = release["actual_released_at"]
    if published_at is None:
        return None

    candidates = [
        schedule
        for schedule in schedules
        if schedule["release_type"] == release["release_type"]
        and abs((schedule["scheduled_at"] - published_at).total_seconds())
        <= max_gap_hours * 3600
    ]
    if not candidates:
        return None

    matched = min(
        candidates,
        key=lambda schedule: abs(
            (schedule["scheduled_at"] - published_at).total_seconds()
        ),
    ).copy()
    matched.update(
        {
            "actual_released_at": published_at,
            "reference_period": release["reference_period"],
            "reference_period_source": release["reference_period_source"],
            "status": "PUBLISHED",
            "source_url": release["source_url"] or matched["source_url"],
            "rss_external_id": release["rss_external_id"],
            "rss_title": release["rss_title"],
        }
    )
    return matched


def collect_release_sync_results(schedules=None, releases=None):
    schedules = fetch_release_schedule() if schedules is None else schedules
    releases = fetch_published_releases() if releases is None else releases
    matched = []
    unmatched = []

    for release in releases:
        merged = merge_schedule_and_release(schedules, release)
        if merged is None:
            unmatched.append(release)
        else:
            matched.append(merged)

    return {"matched": matched, "unmatched": unmatched}


def collect_merged_releases():
    return collect_release_sync_results()["matched"]


if __name__ == "__main__":
    results = collect_release_sync_results()
    for item in results["matched"]:
        print(item)
    for item in results["unmatched"]:
        print("UNMATCHED:", item)
