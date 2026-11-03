"""Pure planning from an explicitly reviewed, hash-pinned session calendar.

No holiday guesses, network, DB changes or provider invocation. Every calendar day
must be either a session with an aware availability time or an explained closure.
"""

from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlsplit

from pipeline.durable_requests import CoordinationError, canonical_hash


def eligible_sessions(calendar, expected_hash, as_of):
    if (
        canonical_hash(calendar) != expected_hash
        or calendar.get("version") != "sessions-v1"
    ):
        raise CoordinationError("contract_conflict")
    uri = urlsplit(calendar["sourceUrl"])
    if (
        uri.scheme != "https"
        or uri.hostname
        not in {"www.nyse.com", "www.nasdaq.com", "www.nasdaqtrader.com"}
        or uri.username
        or uri.password
        or uri.fragment
    ):
        raise CoordinationError("invalid_contract")
    if as_of.tzinfo is None or as_of > datetime.now(timezone.utc):
        raise CoordinationError("invalid_contract")
    start, end = (
        date.fromisoformat(calendar["start"]),
        date.fromisoformat(calendar["end"]),
    )
    if not 0 <= (end - start).days <= 15000:
        raise CoordinationError("invalid_contract")
    days = calendar["days"]
    expected = {
        (start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)
    }
    if set(days) != expected:
        raise CoordinationError("review_required")
    available, pending = [], []
    for day in sorted(days):
        entry = days[day]
        if entry["state"] == "CLOSED":
            if not entry.get("reason"):
                raise CoordinationError("review_required")
        elif entry["state"] == "SESSION":
            close, ready = (
                datetime.fromisoformat(entry["closeAt"]),
                datetime.fromisoformat(entry["availableAt"]),
            )
            if (
                close.tzinfo is None
                or ready.tzinfo is None
                or ready < close
                or abs((close.date() - date.fromisoformat(day)).days) > 1
            ):
                raise CoordinationError("invalid_contract")
            (available if ready <= as_of else pending).append(day)
        else:
            raise CoordinationError("review_required")
    return available, pending


def collection_plan(
    calendar, calendar_hash, observed_dates, *, as_of, overlap=5, adjusted=False
):
    if type(overlap) is not int or not 1 <= overlap <= 60 or type(adjusted) is not bool:
        raise CoordinationError("invalid_contract")
    available, pending = eligible_sessions(calendar, calendar_hash, as_of)
    observed = list(observed_dates)
    if len(set(observed)) != len(observed) or not set(observed).issubset(available):
        raise CoordinationError("contract_conflict")
    recent = available[-overlap:]
    old_gaps = sorted(set(available) - set(observed) - set(recent))
    if adjusted:
        requests = available  # Full new generation; never splice daily adjusted pages.
        mode = "ADJUSTED_FULL_GENERATION"
    else:
        requests = recent
        mode = "UNADJUSTED_OVERLAP_WITH_SEPARATE_GAPS"
    body = {
        "version": "collection-plan-v1",
        "calendarHash": calendar_hash,
        "asOf": as_of.isoformat(),
        "mode": mode,
        "requestedDates": requests,
        "gapDates": [] if adjusted else old_gaps,
        "deferredDates": pending,
        "observedHash": canonical_hash(sorted(observed)),
        "emptyResponseMeans": "REVIEW_NOT_DELISTED",
    }
    return {**body, "generation": canonical_hash(body)}


def compare_generation(previous, candidate, expected_dates):
    def index(rows):
        out = {}
        for row in rows:
            if row["date"] in out:
                raise CoordinationError("contract_conflict")
            out[row["date"]] = row
        return out

    old, new = index(previous), index(candidate)
    if set(new) != set(expected_dates) or not set(old).issubset(new):
        raise CoordinationError("review_required")
    changed = sorted(day for day in old if old[day] != new[day])
    return {
        "previousHash": canonical_hash(previous),
        "candidateHash": canonical_hash(candidate),
        "changedPastDates": changed,
        "addedDates": sorted(set(new) - set(old)),
        "historicalChangeRequiresReview": bool(changed),
    }
