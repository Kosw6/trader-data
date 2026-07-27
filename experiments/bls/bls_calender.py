import re
from datetime import datetime, timezone

import requests
from icalendar import Calendar


ICS_URL = "https://www.bls.gov/schedule/news_release/bls.ics"
HEADERS = {"User-Agent": "TraderDataBot/1.0"}

RELEASE_TYPE_RULES = {
    "consumer price index": "CPI",
    "producer price index": "PPI",
    "employment situation": "EMPLOYMENT",
    "job openings and labor turnover": "JOLTS",
    "employment cost index": "ECI",
    "import and export price": "IMPORT_EXPORT_PRICE",
}

TRACKED_RELEASE_TYPES = set(RELEASE_TYPE_RULES.values())


def normalize_release_type(title):
    normalized = title.lower()
    for keyword, release_type in RELEASE_TYPE_RULES.items():
        if keyword in normalized:
            return release_type
    return "OTHER"


def parse_frequency(title):
    match = re.search(r"\((monthly|quarterly|annual)\)", title, re.IGNORECASE)
    return match.group(1).upper() if match else None


def decode_optional_datetime(event, field):
    if not event.get(field):
        return None

    value = event.decoded(field)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return None


def parse_calendar_event(event):
    title = str(event.get("summary", ""))
    scheduled_at = event.decoded("dtstart")

    if not isinstance(scheduled_at, datetime):
        raise ValueError(f"BLS event has no datetime DTSTART: {title}")
    if scheduled_at.tzinfo is None:
        scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)

    return {
        "provider": "BLS",
        "external_id": str(event.get("uid")),
        "release_type": normalize_release_type(title),
        "title": title,
        "frequency": parse_frequency(title),
        "scheduled_at": scheduled_at.astimezone(timezone.utc),
        "actual_released_at": None,
        "reference_period": None,
        "sequence_number": int(event.get("sequence", 0)),
        "status": "SCHEDULED",
        "source_url": str(event.get("url")) if event.get("url") else None,
        "source_updated_at": decode_optional_datetime(event, "last-modified"),
        "collected_at": datetime.now(timezone.utc),
        "raw_payload": event.to_ical().decode("utf-8"),
    }


def fetch_release_schedule(include_other=False):
    response = requests.get(ICS_URL, headers=HEADERS, timeout=10)
    response.raise_for_status()

    calendar = Calendar.from_ical(response.content)
    schedules = []

    for event in calendar.walk("VEVENT"):
        schedule = parse_calendar_event(event)
        if include_other or schedule["release_type"] in TRACKED_RELEASE_TYPES:
            schedules.append(schedule)

    return schedules


if __name__ == "__main__":
    for item in fetch_release_schedule():
        print(item)
