"""명시한 종목의 기존 DB 보유 기간으로 재개 가능한 수정주가 백필."""
import argparse
import json
import math
import os
import re
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
from urllib.parse import quote
import psycopg2
from psycopg2.extensions import parse_dsn
from dotenv import load_dotenv
from collectors.common import content_hash
from collectors.kis_client import KISClient, normalize_daily_price
from collectors.kis_diagnostics import error_metadata
from workers.kis_price_worker import load_asset
from backfills.chart_snapshot_store import publish_snapshot
from backfills.chart_snapshot_selection import select_snapshot_bars, validate_bars as validate_bars

SYMBOLS=("NVDA","AAPL","MSFT","AMZN","GOOGL","TSLA")


def symbol_folder(symbol):
    # Preferred shares/units may contain '/'. Keep provider identity intact but
    # never interpret it as a directory separator or path traversal.
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9./\-]{0,29}",symbol):
        raise ValueError("안전한 공급자 심볼이 필요합니다.")
    return quote(symbol,safe="")


def save(path,value):
    temporary=path.with_suffix(path.suffix+".tmp")
    temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    temporary.replace(path)


def raw_dates(connection,asset_id):
    with connection.cursor() as cur:
        cur.execute("""SELECT (bar_time AT TIME ZONE 'UTC')::date FROM price_bar WHERE asset_id=%s
            AND source='KIS' AND "interval"='1D' AND display_policy='FULL_OHLC' ORDER BY bar_time""",(asset_id,))
        return [row[0] for row in cur.fetchall()]


def close_only_dates(connection, asset_id, start, end):
    """다른 자산/공급자/주기의 정책이 섞이지 않도록 동일 일봉 범위만 읽는다."""
    with connection.cursor() as cur:
        cur.execute('''SELECT (bar_time AT TIME ZONE 'UTC')::date FROM price_bar
            WHERE asset_id=%s AND source='KIS' AND "interval"='1D'
            AND display_policy='CLOSE_ONLY'
            AND bar_time >= (%s::date::timestamp AT TIME ZONE 'UTC')
            AND bar_time < ((%s::date + 1)::timestamp AT TIME ZONE 'UTC') ORDER BY bar_time''',
            (asset_id, start, end))
        return [row[0] for row in cur.fetchall()]


def main(argv=None, shared_client=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory",type=Path)
    parser.add_argument("--apply",action="store_true")
    parser.add_argument("--symbols",nargs="+",default=list(SYMBOLS))
    args=parser.parse_args(argv)
    symbols=list(dict.fromkeys(args.symbols))
    for symbol in symbols: symbol_folder(symbol)
    dsn=os.environ["LOCAL_PRICE_DATABASE_URL"]
    if parse_dsn(dsn).get("host") not in {"localhost","127.0.0.1"}: raise ValueError("로컬 DB 전용입니다.")
    args.directory.mkdir(parents=True,exist_ok=True)
    state_file=args.directory/"state.json"
    state=json.loads(state_file.read_text(encoding="utf-8")) if state_file.exists() else {"createdAt":datetime.now(timezone.utc).isoformat(),"assets":{}}
    if datetime.now(timezone.utc)-datetime.fromisoformat(state["createdAt"])>timedelta(hours=6):
        raise ValueError("6시간이 지난 수집 세대는 새 디렉터리로 시작해야 합니다.")
    load_dotenv(Path(__file__).resolve().parents[1]/".env",override=False)
    client=(shared_client or KISClient.from_env(timeout=20,min_request_interval=1,max_retries=0)) if args.apply else None
    try:
        with psycopg2.connect(dsn) as conn:
            for symbol in symbols:
                entry=state["assets"].get(symbol,{})
                if entry.get("status")=="PUBLISHED": continue
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT asset_id FROM asset_provider_symbol WHERE provider='KIS' AND provider_symbol=%s AND is_current",(symbol,))
                        identities=cur.fetchall()
                        if len(identities)!=1: raise ValueError("종목 식별이 유일하지 않습니다.")
                        asset=load_asset(conn,identities[0][0])
                        if not asset or asset["currency"]!="USD": raise ValueError("활성 USD 종목만 허용합니다.")
                    expected=raw_dates(conn,asset["asset_id"])
                    if not expected: raise ValueError("기존 일봉이 없습니다.")
                    start,end=min(expected),max(expected)
                    close_only=close_only_dates(conn,asset["asset_id"],start,end)
                    plan={"assetId":asset["asset_id"],"start":str(start),"end":str(end),"expectedDates":len(expected),"dateHash":content_hash([str(day) for day in expected])}
                    if entry.get("plan") and entry["plan"]!=plan: raise ValueError("기존 계획의 거래일 집합이 변경됐습니다.")
                    entry["plan"]=plan
                    state["assets"][symbol]=entry
                    print(json.dumps({"symbol":symbol,**plan,"estimatedPages":math.ceil(len(expected)/100)}),flush=True)
                    conn.commit()
                    if not args.apply:
                        entry["status"]="PLANNED"; save(state_file,state); continue
                    folder=args.directory/symbol_folder(symbol); folder.mkdir(exist_ok=True)
                    entry['status']='COLLECTING'
                    cursor=end; pages=[]; by_day={}
                    for page_no in range(150):
                        page_file=folder/f"page-{page_no:03d}.json"
                        if page_file.exists():
                            page=json.loads(page_file.read_text(encoding="utf-8"))
                            if page["cursor"]!=str(cursor) or page["symbol"]!=symbol or page["adjusted"] is not True:
                                raise ValueError("체크포인트 요청 계약이 다릅니다.")
                        else:
                            entry.update(attemptedCursor=str(cursor), attemptedPage=page_no,
                                         attemptedIntervalSeconds=client.min_request_interval)
                            result=client.get_daily_price_page(asset["provider_exchange_code"],asset["provider_symbol"],cursor,adjusted=True)
                            page={"symbol":symbol,"adjusted":True,"cursor":str(cursor),"collectedAt":datetime.now(timezone.utc).isoformat(),"payload":result["raw_payload"]}
                            save(page_file,page)
                        parsed=[normalize_daily_price(row) for row in page["payload"].get("output2",[]) if row.get("xymd")]
                        if not parsed: raise ValueError("기간 완료 전 빈 공급자 페이지입니다.")
                        if any(bar["date"]>cursor for bar in parsed): raise ValueError("커서 이후 거래일이 반환됐습니다.")
                        for bar in parsed:
                            if start<=bar["date"]<=end:
                                if bar["date"] in by_day: raise ValueError("페이지 간 중복 거래일입니다.")
                                by_day[bar["date"]]=bar
                        pages.append(page)
                        entry.update(status="COLLECTING",pages=len(pages),rows=len(by_day),cursor=str(cursor))
                        save(state_file,state)
                        print(f"{symbol} page={len(pages)} rows={len(by_day)} cursor={cursor}",flush=True)
                        oldest=min(bar["date"] for bar in parsed)
                        if oldest<=start: break
                        next_cursor=oldest-timedelta(days=1)
                        if next_cursor>=cursor: raise ValueError("커서가 과거로 이동하지 않습니다.")
                        cursor=next_cursor
                    else: raise ValueError("150페이지 한도를 초과했습니다.")
                    bars=[by_day[day] for day in sorted(by_day)]
                    entry["status"]="VALIDATING"
                    bars,validation=select_snapshot_bars(bars,expected,start,end,close_only)
                    save(folder/"selection-review.json",validation)
                    if raw_dates(conn,asset["asset_id"])!=expected: raise ValueError("수집 중 원본 거래일 집합이 변경됐습니다.")
                    if close_only_dates(conn,asset["asset_id"],start,end)!=close_only:
                        raise ValueError("수집 중 CLOSE_ONLY 거래일 정책이 변경됐습니다.")
                    collected=datetime.fromisoformat(pages[0]["collectedAt"])
                    payload={"provider":"KIS","adjusted":True,"symbol":symbol,"assetId":asset["asset_id"],
                        "start":str(start),"end":str(end),"pages":pages,"collectionStartedAt":state["createdAt"],"validation":validation}
                    entry["status"]="PUBLISHING"
                    snapshot_id=publish_snapshot(conn,asset,payload,bars,start,end,collected,folder/"publication")
                    conn.commit()
                    entry.update(status="PUBLISHED",snapshotId=snapshot_id,validation=validation)
                    save(state_file,state)
                    print(f"PUBLISHED {symbol} snapshot={snapshot_id} rows={len(bars)}",flush=True)
                except Exception as error:
                    conn.rollback()
                    code=re.search(r"daily price failed: ([A-Za-z0-9_]+)",str(error))
                    entry.update(errorPhase=entry.get("status"),status="FAILED",errorType=type(error).__name__,providerErrorCode=code.group(1) if code else None,
                                 failedAt=datetime.now(timezone.utc).isoformat())
                    entry.update(error_metadata(error))
                    safe_reasons={
                        '기간 완료 전 빈 공급자 페이지입니다.':'PROVIDER_EMPTY',
                        '조회 기간 끝점 또는 기존 거래일이 누락됐습니다.':'COVERAGE_MISMATCH',
                        '모든 수정 일봉의 OHLCV 검증이 필요합니다.':'OHLCV_INVALID',
                        '게시 대상 수정 일봉의 OHLCV가 유효하지 않습니다.':'OHLCV_INVALID',
                        '종목 식별이 유일하지 않습니다.':'SYMBOL_NOT_UNIQUE',
                        '페이지 간 중복 거래일입니다.':'DUPLICATE_DATE',
                    }
                    entry['failureReason']=safe_reasons.get(str(error),'REVIEW_REQUIRED')
                    state["assets"][symbol]=entry; save(state_file,state)
                    print(f"FAILED {symbol}: {type(error).__name__} code={entry['providerErrorCode']}",flush=True)
            if args.apply and any(state["assets"].get(symbol,{}).get("status")!="PUBLISHED" for symbol in symbols):
                raise SystemExit(1)
    finally:
        if client and shared_client is None: client.session.close()


if __name__=="__main__": main()
