"""수집·검증을 마친 단일 공급자 세대를 별도 차트 복사본으로 게시한다."""
from collectors.common import LocalPipelineStorage, content_hash
from workers.kis_price_worker import build_price_rows, upsert_source_object, upsert_price_bar_lineage
from workers.price_persistence import persist_price_rows


def publish_snapshot(connection, asset, payload, bars, start, end, collected, directory):
    if payload.get("adjusted") is not True or payload.get("provider") != "KIS":
        raise ValueError("수정주가 원문이 필요합니다.")
    options={"adjusted":True,"source":"KIS","interval":"1D","currency":asset["currency"]}
    rows,statuses=build_price_rows(asset,bars,options,collected)
    if not rows or len(rows)!=len(bars) or any(s["status"]!="VALID" for s in statuses):
        raise ValueError("모든 수정 일봉의 OHLCV 검증이 필요합니다.")
    storage=LocalPipelineStorage(directory)
    storage.storage_backend,storage.storage_base_uri="LOCAL",""
    origin=storage.write_raw_json("KIS","PRICE_BAR",f"{asset['asset_id']}-{asset['ticker']}",payload,collected)
    with connection.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout='120s'")
        sid=upsert_source_object(cur,origin)
        records=persist_price_rows(connection,rows,"KIS","1D",asset["currency"],sid)
        upsert_price_bar_lineage(cur,sid,records,collected)
        digest=content_hash([row["content_hash"] for row in rows])
        cur.execute("SELECT id,content_hash FROM price_chart_snapshot WHERE asset_id=%s AND source_object_id=%s",(asset["asset_id"],sid))
        existing=cur.fetchone()
        if existing:
            if existing[1]!=digest: raise ValueError("게시된 세대의 해시가 달라졌습니다.")
            snapshot_id=existing[0]
        else:
            cur.execute("""INSERT INTO price_chart_snapshot(asset_id,source_object_id,source,currency,basis,volume_basis,
                coverage_start,coverage_end,collected_at,row_count,content_hash)
                VALUES(%s,%s,'KIS',%s,'KIS_PROVIDER_ADJUSTED','PROVIDER_REPORTED',%s,%s,%s,%s,%s) RETURNING id""",
                (asset["asset_id"],sid,asset["currency"],start,end,collected,len(rows),digest))
            snapshot_id=cur.fetchone()[0]
            cur.execute("""INSERT INTO price_chart_snapshot_bar(snapshot_id,trade_date,candidate_id,open,high,low,close,volume)
                SELECT %s,trade_date,id,open,high,low,close,volume FROM price_adjustment_candidate
                WHERE source_object_id=%s AND asset_id=%s AND source='KIS' AND "interval"='1D' AND currency=%s""",
                (snapshot_id,sid,asset["asset_id"],asset["currency"]))
        cur.execute("SELECT count(*) FROM price_chart_snapshot_bar WHERE snapshot_id=%s",(snapshot_id,))
        if cur.fetchone()[0]!=len(rows): raise ValueError("게시 행 수가 일치하지 않습니다.")
    return snapshot_id
