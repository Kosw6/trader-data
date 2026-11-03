"""NVDA 분할 전후 제한된 비교 수집. DB 쓰기·검색 승인·주문 기능은 없다."""

import json
import argparse
from datetime import date, datetime, timezone
from pathlib import Path

from collectors.kis_client import KISAPIError, KISClient
from dotenv import load_dotenv
from requests.exceptions import RequestException


def compare(raw_bars, adjusted_bars):
    raw = {bar["date"]: bar["close"] for bar in raw_bars}
    adjusted = {bar["date"]: bar["close"] for bar in adjusted_bars}
    rows = []
    for day in sorted(raw.keys() | adjusted.keys()):
        left, right = raw.get(day), adjusted.get(day)
        ratio = (
            left / right
            if left is not None and right is not None and right > 0
            else None
        )
        rows.append(
            {
                "tradeDate": day.isoformat(),
                "unadjustedRequestClose": str(left) if left is not None else None,
                "adjustedRequestClose": str(right) if right is not None else None,
                "ratio": str(ratio) if ratio is not None else None,
            }
        )
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2024,6,3))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2024,6,14))
    args = parser.parse_args()
    if args.end < args.start or (args.end-args.start).days > 31:
        parser.error("비교 수집은 최대 32일 범위만 허용합니다.")
    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env", override=False)
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = root / "pipeline_output" / "adjustment_probe" / run
    output.mkdir(parents=True, exist_ok=False)
    # 가격 조회는 최대 두 번이며 인증이 필요하면 토큰을 한 번 발급한다.
    client = KISClient.from_env(timeout=15, min_request_interval=1, max_retries=0)
    report = {
        "symbol": "NVDA",
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "approval": "NOT_APPROVED",
        "results": [],
        "errors": [],
    }
    observed = {}
    try:
        for adjusted in (False, True):
            try:
                result = client.get_daily_prices(
                    "NAS",
                    "NVDA",
                    args.start,
                    args.end,
                    adjusted=adjusted,
                    max_pages=1,
                )
                name = "adjusted" if adjusted else "unadjusted"
                payload = {
                    "provider": "KIS",
                    "symbol": "NVDA",
                    "adjusted": adjusted,
                    "collectedAt": datetime.now(timezone.utc).isoformat(),
                    "pages": result["raw_pages"],
                }
                (output / f"{name}.json").write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                    encoding="utf-8",
                )
                observed[adjusted] = result["bars"]
                report["results"].append(
                    {
                        "adjusted": adjusted,
                        "rows": len(result["bars"]),
                        "rawFile": f"{name}.json",
                    }
                )
            except (KISAPIError, RequestException, ValueError, OSError) as error:
                # 공급자 오류 원문/요청 헤더에는 비밀값이 섞일 수 있어 분류만 남긴다.
                report["errors"].append(
                    {"adjusted": adjusted, "type": type(error).__name__}
                )
                break
        report["comparison"] = compare(observed.get(False, []), observed.get(True, []))
    finally:
        client.session.close()
        (output / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print("Report directory:", output)
    if report["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
