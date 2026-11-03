"""저장된 비교 원문을 OHLCV까지 검토한다. 네트워크·DB 쓰기·가격 승인 없음."""
import argparse
import json
from datetime import date
from pathlib import Path
from collectors.kis_client import normalize_daily_price


def review(raw_bars, adjusted_bars):
    def index(bars):
        values = {}
        for bar in bars:
            if bar["date"] in values:
                raise ValueError("같은 거래일의 중복 가격은 임의 선택할 수 없습니다.")
            values[bar["date"]] = bar
        return values

    raw, adjusted = index(raw_bars), index(adjusted_bars)
    rows = []
    for day in sorted(raw.keys() | adjusted.keys()):
        left, right = raw.get(day), adjusted.get(day)
        prices_valid = bool(left and right) and all(
            all(bar.get(key) is not None and bar[key] > 0 for key in ("open", "high", "low", "close"))
            and bar["low"] <= min(bar["open"], bar["close"])
            and bar["high"] >= max(bar["open"], bar["close"])
            for bar in (left, right)
        )
        rows.append({
            "tradeDate": day.isoformat(), "bothPresent": bool(left and right),
            "ohlcValid": prices_valid,
            "raw": {k: str(left[k]) if left[k] is not None else None for k in ("open", "high", "low", "close", "volume")} if left else None,
            "adjusted": {k: str(right[k]) if right[k] is not None else None for k in ("open", "high", "low", "close", "volume")} if right else None,
            "volumeComparison": "UNKNOWN" if not left or not right or left.get("volume") is None or right.get("volume") is None
                else "SAME" if left["volume"] == right["volume"] else "DIFFERENT",
        })
    return {
        "approval": "NOT_APPROVED", "rawCount": len(raw), "adjustedCount": len(adjusted),
        "matchingDates": sum(row["bothPresent"] for row in rows),
        "validOhlcPairs": sum(row["ohlcValid"] for row in rows),
        "rows": rows,
        "limitation": "표본의 수치 검토이며 분할 전용·총수익률·거래량 조정 공식을 입증하지 않는다.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    metadata = json.loads((args.directory / "report.json").read_text(encoding="utf-8"))
    start, end = date.fromisoformat(metadata["start"]), date.fromisoformat(metadata["end"])
    observations = []
    for name, expected in (("unadjusted", False), ("adjusted", True)):
        source = json.loads((args.directory / f"{name}.json").read_text(encoding="utf-8"))
        if source.get("adjusted") is not expected or source.get("symbol") != metadata["symbol"]:
            raise ValueError("원문의 종목 또는 수정주가 옵션이 비교 계약과 다릅니다.")
        bars = [normalize_daily_price(row) for page in source["pages"] for row in page["payload"].get("output2", []) if row.get("xymd")]
        observations.append([bar for bar in bars if start <= bar["date"] <= end])
    result = review(*observations)
    # 원문 파일은 수정하지 않고 별도 검토 결과만 보존한다.
    (args.directory / "ohlcv-review.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
