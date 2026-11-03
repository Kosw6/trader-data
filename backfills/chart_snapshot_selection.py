"""수집 원문과 차트 게시 대상을 구분한다. 가격을 보정하거나 원본을 삭제하지 않는다."""

from backfills.price_bar_invalid_repair import invalid_reasons


def validate_bars(bars, expected, start, end):
    """필수 거래일의 완전성과 범위·중복 계약은 제외 처리 전에 검사한다."""
    days = [bar["date"] for bar in bars]
    if not days or len(days) != len(set(days)):
        raise ValueError("중복 또는 빈 거래일 집합입니다.")
    required = set(expected)
    if not required or len(required) != len(expected):
        raise ValueError("필수 거래일 집합이 비어 있거나 중복됐습니다.")
    if (
        min(days) != start
        or max(days) != end
        or min(required) != start
        or max(required) != end
        or not required.issubset(days)
    ):
        raise ValueError("조회 기간 끝점 또는 기존 거래일이 누락됐습니다.")
    return {
        "rows": len(days),
        "existingDates": len(required),
        "additionalProviderDates": len(set(days) - required),
    }


def select_snapshot_bars(bars, expected, start, end, close_only):
    """DB에서 확인한 추가 CLOSE_ONLY만 분리하고 나머지 모든 일봉을 엄격히 검증한다.

    close_only는 공급자 응답이나 추측이 아니라 동일 자산의 기존 DB 정책이어야 한다.
    기존 정책을 임의 승격하지 않으므로 OHLC가 유효해도 CLOSE_ONLY 추가일은 제외한다.
    """
    received = validate_bars(bars, expected, start, end)
    required = set(expected)
    close_only = set(close_only)
    if required & close_only:
        raise ValueError("필수 거래일과 CLOSE_ONLY 정책이 겹칩니다.")
    selected, excluded = [], []
    for bar in bars:
        reasons = list(
            invalid_reasons(
                bar.get("open"),
                bar.get("high"),
                bar.get("low"),
                bar.get("close"),
                bar.get("volume"),
            )
        )
        if bar["date"] in close_only:
            excluded.append(
                {
                    "date": str(bar["date"]),
                    "policy": "CLOSE_ONLY",
                    "reason": "EXISTING_CLOSE_ONLY_ADDITIONAL_DATE",
                    "ohlcvReasons": reasons,
                }
            )
        else:
            if reasons:
                raise ValueError("게시 대상 수정 일봉의 OHLCV가 유효하지 않습니다.")
            selected.append(bar)
    selected.sort(key=lambda bar: bar["date"])
    validation = validate_bars(selected, expected, start, end)
    validation.update(
        {
            "selectionPolicy": "REQUIRED_FULL_OHLC_AND_VALID_ADDITIONAL_V1",
            "receivedRows": received["rows"],
            "receivedAdditionalProviderDates": received["additionalProviderDates"],
            "excludedRows": len(excluded),
            "excludedCloseOnlyDates": excluded,
        }
    )
    return selected, validation
