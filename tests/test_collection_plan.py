from datetime import datetime, timezone

import pytest
from pipeline.collection_plan import collection_plan, compare_generation
from pipeline.durable_requests import CoordinationError, canonical_hash


def calendar():
    return {
        "version": "sessions-v1",
        "start": "2026-01-01",
        "end": "2026-01-05",
        "sourceUrl": "https://www.nyse.com/markets/hours-calendars",
        "days": {
            "2026-01-01": {"state": "CLOSED", "reason": "synthetic approved closure"},
            "2026-01-02": {
                "state": "SESSION",
                "closeAt": "2026-01-02T21:00:00+00:00",
                "availableAt": "2026-01-02T22:00:00+00:00",
            },
            "2026-01-03": {"state": "CLOSED", "reason": "weekend"},
            "2026-01-04": {"state": "CLOSED", "reason": "weekend"},
            "2026-01-05": {
                "state": "SESSION",
                "closeAt": "2026-01-05T21:00:00+00:00",
                "availableAt": "2026-01-05T22:00:00+00:00",
            },
        },
    }


def test_overlap_internal_gap_and_full_adjusted_generation():
    c = calendar()
    at = datetime(2026, 1, 6, tzinfo=timezone.utc)
    plan = collection_plan(c, canonical_hash(c), [], as_of=at, overlap=1)
    assert plan["requestedDates"] == ["2026-01-05"]
    assert plan["gapDates"] == ["2026-01-02"]
    adjusted = collection_plan(
        c, canonical_hash(c), [], as_of=at, overlap=1, adjusted=True
    )
    assert (
        adjusted["requestedDates"] == ["2026-01-02", "2026-01-05"]
        and not adjusted["gapDates"]
    )


def test_provider_delay_defers_session_and_generation_is_stable():
    c = calendar()
    at = datetime(2026, 1, 5, 21, 30, tzinfo=timezone.utc)
    first = collection_plan(c, canonical_hash(c), [], as_of=at)
    assert first["requestedDates"] == ["2026-01-02"] and first["deferredDates"] == [
        "2026-01-05"
    ]
    assert first == collection_plan(c, canonical_hash(c), [], as_of=at)


@pytest.mark.parametrize(
    "bad", ["hash", "missing_day", "unknown", "naive", "observed_closed"]
)
def test_unknown_calendar_never_guesses_trading_days(bad):
    c = calendar()
    at = datetime(2026, 1, 6, tzinfo=timezone.utc)
    observed = []
    if bad == "missing_day":
        del c["days"]["2026-01-03"]
    if bad == "unknown":
        c["days"]["2026-01-03"]["state"] = "UNKNOWN"
    if bad == "naive":
        at = at.replace(tzinfo=None)
    if bad == "observed_closed":
        observed = ["2026-01-03"]
    with pytest.raises(CoordinationError):
        collection_plan(
            c, "0" * 64 if bad == "hash" else canonical_hash(c), observed, as_of=at
        )


def test_corporate_action_changes_are_reported_not_spliced():
    old = [{"date": "2026-01-02", "close": "100"}]
    new = [{"date": "2026-01-02", "close": "50"}, {"date": "2026-01-05", "close": "51"}]
    assert compare_generation(old, new, ["2026-01-02", "2026-01-05"])[
        "historicalChangeRequiresReview"
    ]
    with pytest.raises(CoordinationError):
        compare_generation(old, new[1:], ["2026-01-05"])
