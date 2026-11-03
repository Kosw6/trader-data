from copy import deepcopy
from datetime import date

import pytest
from pipeline.adjusted_refresh import decode_page, sessions, validate_generation


def fixture():
    target = {
        "coverage_start": "2026-10-21",
        "coverage_end": "2026-10-21",
        "provider_symbol": "NVDA",
        "provider_exchange_code": "NAS",
    }
    raw = [
        {"xymd": d, "open": "10", "high": "12", "low": "9", "clos": "11", "tvol": "100"}
        for d in ("20261023", "20261022", "20261021", "20261020")
    ]
    page = {
        "symbol": "NVDA",
        "exchange": "NAS",
        "adjusted": True,
        "cursor": "2026-10-23",
        "payload": {"rt_cd": "0", "output1": {"rsym": "DNASNVDA"}, "output2": raw},
    }
    return target, page


def test_full_generation_not_spliced():
    target, page = fixture()
    bars = validate_generation([page], target, date(2026, 10, 23), [])
    assert [r["date"] for r in bars] == ["2026-10-21", "2026-10-22", "2026-10-23"]
    assert bars[0]["close"] == "11"


@pytest.mark.parametrize(
    "change", ["identity", "future", "duplicate", "business", "schema"]
)
def test_page_identity_and_date_boundaries(change):
    _, page = fixture()
    payload = deepcopy(page["payload"])
    if change == "identity":
        payload["output1"]["rsym"] = "DNASAAPL"
    elif change == "future":
        payload["output2"][0]["xymd"] = "20261026"
    elif change == "duplicate":
        payload["output2"].append(payload["output2"][0])
    elif change == "business":
        payload["rt_cd"] = "1"
    else:
        payload.pop("output2")
    with pytest.raises((ValueError, TypeError)):
        decode_page(payload, "NVDA", "NAS", date(2026, 10, 23))


@pytest.mark.parametrize(
    "change", ["adjusted", "missing", "range", "nan", "negative", "empty"]
)
def test_invalid_generation_is_never_activated(change):
    target, page = fixture()
    if change == "adjusted":
        page["adjusted"] = False
    elif change == "missing":
        page["payload"]["output2"].pop(1)
    elif change == "range":
        page["payload"]["output2"][0]["high"] = "8"
    elif change == "nan":
        page["payload"]["output2"][0]["clos"] = "NaN"
    elif change == "negative":
        page["payload"]["output2"][0]["tvol"] = "-1"
    else:
        page["payload"]["output2"] = []
    with pytest.raises(ValueError):
        validate_generation([page], target, date(2026, 10, 23), [])


def test_calendar_uses_explicit_holiday_and_weekend():
    assert sessions(date(2026, 7, 2), date(2026, 7, 6), ["2026-07-03"]) == [
        date(2026, 7, 2),
        date(2026, 7, 6),
    ]


def test_close_only_is_excluded_and_cannot_replace_endpoint():
    target, page = fixture()
    page["payload"]["output2"][1]["open"] = "0"
    assert (
        len(validate_generation([page], target, date(2026, 10, 23), [], ["2026-10-22"]))
        == 2
    )
    with pytest.raises(ValueError):
        validate_generation([page], target, date(2026, 10, 23), [], ["2026-10-23"])
