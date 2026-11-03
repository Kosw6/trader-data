"""6종목의 백필 게시본을 대조한 뒤 사용자 기본 가격 세대를 원자적으로 전환."""
import argparse
import json
import os
from pathlib import Path
import psycopg2
from psycopg2.extensions import parse_dsn
from collectors.common import content_hash
from backfills.backfill_adjusted_charts import SYMBOLS


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory",type=Path)
    parser.add_argument("--apply",action="store_true")
    parser.add_argument("--exclude",action="append",default=[],help="실패 종목만 명시적으로 보류한다. 조용히 무시하지 않는다.")
    parser.add_argument("--symbols",nargs="+",default=list(SYMBOLS))
    args=parser.parse_args(argv)
    symbols=list(dict.fromkeys(args.symbols))
    if not set(args.exclude).issubset(symbols): raise ValueError("보류 종목은 현재 대상에 포함되어야 합니다.")
    state=json.loads((args.directory/"state.json").read_text(encoding="utf-8"))
    dsn=os.environ["LOCAL_PRICE_DATABASE_URL"]
    if parse_dsn(dsn).get("host") not in {"localhost","127.0.0.1"}: raise ValueError("로컬 DB 전용입니다.")
    ids=[]; assets=[]; report=[]
    with psycopg2.connect(dsn) as conn,conn.cursor() as cur:
        for symbol in symbols:
            entry=state["assets"][symbol]
            if symbol in args.exclude:
                if entry.get("status")!="FAILED": raise ValueError("실패 상태인 종목만 보류할 수 있습니다.")
                continue
            if entry.get("status")!="PUBLISHED": raise ValueError(f"{symbol} 백필이 완료되지 않았습니다.")
            sid,plan=entry["snapshotId"],entry["plan"]
            cur.execute("""SELECT asset_id,coverage_start,coverage_end,row_count,content_hash,source_object_id
                FROM price_chart_snapshot WHERE id=%s AND basis='KIS_PROVIDER_ADJUSTED' AND source='KIS'""",(sid,))
            meta=cur.fetchone()
            if not meta or (meta[0],str(meta[1]),str(meta[2]))!=(plan["assetId"],plan["start"],plan["end"]):
                raise ValueError("계획과 게시 메타데이터가 일치하지 않습니다.")
            cur.execute("""SELECT b.trade_date,c.content_hash,
                (b.open=c.open AND b.high=c.high AND b.low=c.low AND b.close=c.close AND b.volume IS NOT DISTINCT FROM c.volume)
                FROM price_chart_snapshot_bar b JOIN price_adjustment_candidate c ON c.id=b.candidate_id
                WHERE b.snapshot_id=%s AND c.asset_id=%s AND c.source_object_id=%s ORDER BY b.trade_date""",(sid,meta[0],meta[5]))
            rows=cur.fetchall()
            if len(rows)!=meta[3] or len(rows)<plan["expectedDates"] or not all(r[2] for r in rows):
                raise ValueError("후보와 차트 가격의 행 수·OHLCV가 다릅니다.")
            if str(rows[0][0])!=plan["start"] or str(rows[-1][0])!=plan["end"] or content_hash([r[1] for r in rows])!=meta[4]:
                raise ValueError("거래일 끝점 또는 세대 해시가 다릅니다.")
            ids.append(sid); assets.append(meta[0])
            report.append({"symbol":symbol,"snapshotId":sid,"rows":len(rows),"start":str(meta[1]),"end":str(meta[2])})
        if args.apply:
            cur.execute("UPDATE price_chart_snapshot SET default_for_chart=false WHERE asset_id=ANY(%s)",(assets,))
            cur.execute("UPDATE price_chart_snapshot SET default_for_chart=true WHERE id=ANY(%s)",(ids,))
        else: conn.rollback()
    (args.directory/"activation-review.json").write_text(json.dumps({"applied":args.apply,"excluded":args.exclude,"assets":report},ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps({"applied":args.apply,"excluded":args.exclude,"assets":report},ensure_ascii=False,indent=2))


if __name__=="__main__":main()
