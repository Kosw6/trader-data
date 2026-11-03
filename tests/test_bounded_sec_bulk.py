import pytest
from backfills.sec_financial.bounded_bulk import MIN_FREE, pause_reason, validate_targets


def test_scope_accepts_unique_companies():
    validate_targets([{"company_id": 1, "cik": "0000000001"}])


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [{"company_id": 1, "cik": "1"}],
        [{"company_id": 1, "cik": "0000000001"}, {"company_id": 1, "cik": "0000000002"}],
        [{"company_id": 1, "cik": "0000000001"}, {"company_id": 2, "cik": "0000000001"}],
    ],
)
def test_invalid_scope_rejected(rows):
    with pytest.raises(ValueError):
        validate_targets(rows)


def test_safety_boundaries():
    assert pause_reason(MIN_FREE, 2, False) is None
    assert pause_reason(MIN_FREE - 1, 0, False) == "PAUSED_LOW_DISK"
    assert pause_reason(MIN_FREE, 3, False) == "PAUSED_CONSECUTIVE_FAILURES"
    assert pause_reason(MIN_FREE, 0, True) == "PAUSED_STOP_REQUEST"
