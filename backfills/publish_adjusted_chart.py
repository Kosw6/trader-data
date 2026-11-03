"""한정 NVDA 2024-06 검토본 게시. 일반 가격/AI 검색 승인은 변경하지 않는다."""
import argparse
import json
import os
from datetime import date, datetime
from pathlib import Path
import psycopg2
from psycopg2.extensions import parse_dsn
from collectors.common import LocalPipelineStorage, content_hash
from collectors.kis_client import normalize_daily_price
from backfills.price_adjustment_review import review
from workers.kis_price_worker import load_asset, build_price_rows, upsert_source_object, upsert_price_bar_lineage
from workers.price_persistence import persist_price_rows


def validate_series(raw, adjusted, expected_dates):
    result = review(raw, adjusted)
    dates = {bar["date"] for bar in adjusted}
    if not expected_dates or dates != set(expected_dates) or {bar["date"] for bar in raw} != dates:
        raise ValueError("일반 원문·수정 원문·로컬 거래일 집합이 일치하지 않습니다.")
    if result["validOhlcPairs"] != len(dates):
        raise ValueError("모든 거래일의 OHLC 검증이 필요합니다.")
    if any(bar.get("volume") is not None and bar["volume"] < 0 for bar in adjusted):
        raise ValueError("거래량은 음수일 수 없습니다.")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    dsn = os.environ["LOCAL_PRICE_DATABASE_URL"]
    target = parse_dsn(dsn)
    if target.get("host") not in {"localhost", "127.0.0.1"}:
        raise ValueError("이 도구는 로컬 DB에서만 실행할 수 있습니다.")
    meta = json.loads((args.directory / "report.json").read_text(encoding="utf-8"))
    if (meta["symbol"],meta["start"],meta["end"]) != ("NVDA","2024-06-01","2024-06-30") or meta.get("errors"):
        raise ValueError("이번 게시 범위는 정상 수집한 NVDA 2024년 6월로 제한합니다.")
    start, end = date.fromisoformat(meta["start"]), date.fromisoformat(meta["end"])
    sources, series = [], []
    for name, flag in (("unadjusted",False),("adjusted",True)):
        source = json.loads((args.directory / f"{name}.json").read_text(encoding="utf-8"))
        if source.get("adjusted") is not flag or source.get("symbol") != "NVDA" or source.get("provider") != "KIS":
            raise ValueError("수집 옵션·종목·공급자가 맞지 않습니다.")
        bars = [normalize_daily_price(row) for page in source["pages"] for row in page["payload"]["output2"] if row.get("xymd")]
        sources.append(source)
        series.append([bar for bar in bars if start <= bar["date"] <= end])
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout='30s'")
            cur.execute("SELECT asset_id FROM asset_provider_symbol WHERE provider='KIS' AND provider_symbol='NVDA' AND is_current")
            identities = cur.fetchall()
            if len(identities) != 1:
                raise ValueError("종목을 유일하게 찾을 수 없습니다.")
            asset = load_asset(conn,identities[0][0])
            if not asset or asset["currency"] != "USD":
                raise ValueError("활성 USD 종목이 필요합니다.")
            cur.execute("""SELECT (bar_time AT TIME ZONE 'UTC')::date FROM price_bar
                WHERE asset_id=%s AND source='KIS' AND "interval"='1D' AND display_policy='FULL_OHLC'
                  AND bar_time >= %s::date AT TIME ZONE 'UTC' AND bar_time < (%s::date+1) AT TIME ZONE 'UTC'""",(asset["asset_id"],start,end))
            validation = validate_series(*series,[row[0] for row in cur.fetchall()])
            print(json.dumps({"dates":validation["matchingDates"],"validOhlc":validation["validOhlcPairs"],"apply":args.apply}))
            if not args.apply:
                conn.rollback()
                return
            collected = datetime.fromisoformat(sources[1]["collectedAt"])
            options = {"adjusted":True,"source":"KIS","interval":"1D","currency":"USD"}
            rows, statuses = build_price_rows(asset,series[1],options,collected)
            if len(rows) != len(series[1]) or any(s["status"] != "VALID" for s in statuses):
                raise ValueError("변환 검증에 실패했습니다.")
            storage = LocalPipelineStorage(args.directory / "publication")
            storage.storage_backend, storage.storage_base_uri = "LOCAL", ""
            origin = storage.write_raw_json("KIS","PRICE_BAR",f"{asset['asset_id']}-NVDA",sources[1],collected)
            source_id = upsert_source_object(cur,origin)
            records = persist_price_rows(conn,rows,"KIS","1D","USD",source_id)
            upsert_price_bar_lineage(cur,source_id,records,collected)
            digest = content_hash([row["content_hash"] for row in rows])
            cur.execute("SELECT id,content_hash FROM price_chart_snapshot WHERE asset_id=%s AND source_object_id=%s",(asset["asset_id"],source_id))
            existing = cur.fetchone()
            if existing:
                if existing[1] != digest:
                    raise ValueError("게시된 원문의 해시가 다릅니다.")
                snapshot_id = existing[0]
            else:
                cur.execute("""INSERT INTO price_chart_snapshot(asset_id,source_object_id,source,currency,basis,volume_basis,
                    coverage_start,coverage_end,collected_at,row_count,content_hash)
                    VALUES (%s,%s,'KIS','USD','KIS_PROVIDER_ADJUSTED','PROVIDER_REPORTED',%s,%s,%s,%s,%s) RETURNING id""",
                    (asset["asset_id"],source_id,start,end,collected,len(rows),digest))
                snapshot_id = cur.fetchone()[0]
                cur.execute("""INSERT INTO price_chart_snapshot_bar(snapshot_id,trade_date,candidate_id,open,high,low,close,volume)
                    SELECT %s,trade_date,id,open,high,low,close,volume FROM price_adjustment_candidate
                    WHERE source_object_id=%s AND asset_id=%s AND source='KIS' AND "interval"='1D' AND currency='USD'""",
                    (snapshot_id,source_id,asset["asset_id"]))
            cur.execute("SELECT count(*) FROM price_chart_snapshot_bar WHERE snapshot_id=%s",(snapshot_id,))
            if cur.fetchone()[0] != len(rows):
                raise ValueError("스냅샷 행 수가 일치하지 않습니다.")
        conn.commit()
        print(json.dumps({"snapshotId":snapshot_id,"rows":len(rows),"rawPriceChanged":False,"aiSearchApproved":False}))


if __name__ == "__main__":
    main()
