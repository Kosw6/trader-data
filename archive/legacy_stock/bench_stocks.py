#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
보관 전용: 폐기한 stock 스키마용 MongoDB vs PostgreSQL 벤치마크. 현행 실행 금지.
- 동일/유사 인덱스 전제:
  Mongo: {symb:1, timestamp:1}, (옵션) {timestamp:1}
  Postgres: (symb, "timestamp"), (symb, "timestamp" DESC), (옵션) "timestamp" DESC (+ 커버링 INCLUDE 권장)
- 워크로드:
  A) 최신 1건 (심볼별)
  B) 구간 조회 (정렬 포함)
  C) 일별 집계 (AVG(close), SUM(volume))
- 보강:
  * 동시성 (ThreadPoolExecutor)
  * Cold/Warm 분리 측정(간이 캐시 교란)
  * 키 분포: uniform/zipf
  * EXPLAIN (Mongo .explain("executionStats") / PG EXPLAIN ANALYZE)
  * 통계: n, avg, p50, p95, min, max, 실패율, 평균 반환행수
"""
import os, time, statistics as stats, argparse, random, math, sys
from datetime import datetime, timedelta, timezone
from dateutil.parser import isoparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from pymongo import MongoClient, ASCENDING, DESCENDING
import psycopg2, psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

# ---------- 유틸 ----------

def pct(arr, p):
    if not arr: return None
    arr = sorted(arr); k = (len(arr)-1)*(p/100.0); f = int(k); c = min(f+1, len(arr)-1)
    return arr[int(k)] if f==c else arr[f]*(c-k)+arr[c]*(k-f)

def ms(sec): return round(sec*1000.0, 2)

def ensure_utc_iso(s):
    # 'YYYY-MM-DD' 또는 ISO 문자열을 UTC로 정규화
    if len(s) == 10:
        dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    else:
        dt = isoparse(s)
        dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    return dt

def summarize(title, m_times, p_times, m_rows=None, p_rows=None, meta=None):
    def stat(a):
        return {
            "n": len(a),
            "avg_ms": ms(stats.fmean(a)) if a else None,
            "p50_ms": ms(pct(a,50)) if a else None,
            "p95_ms": ms(pct(a,95)) if a else None,
            "min_ms": ms(min(a)) if a else None,
            "max_ms": ms(max(a)) if a else None,
        }
    def rowstat(r):
        return None if r is None else round(sum(r)/len(r), 2) if r else 0.0
    print(f"\n=== {title} ===")
    if meta:
        print("Meta :", meta)
    print("MongoDB :", stat(m_times), ("avg_rows="+str(rowstat(m_rows)) if m_rows is not None else ""))
    print("Postgres:", stat(p_times), ("avg_rows="+str(rowstat(p_rows)) if p_rows is not None else ""))

def zipf_weights(n, alpha=1.1):
    ranks = [i+1 for i in range(n)]
    weights = [1.0 / (r**alpha) for r in ranks]
    s = sum(weights)
    return [w/s for w in weights]

def pick_symbols_uniform(all_syms, k):
    sample = list(all_syms)
    random.shuffle(sample)
    return sample[:k]

def pick_symbols_zipf(all_syms, k, alpha=1.1):
    syms_sorted = sorted(all_syms)  # 상위가 더 자주 뽑히도록 고정 순서
    weights = zipf_weights(len(syms_sorted), alpha=alpha)
    return random.choices(syms_sorted, weights=weights, k=k)

def cache_bust_mongo(mcoll, bytes_target=64*1024*1024):
    # 캐시 교란: 큰 범위 스캔 몇 번 (정확한 OS 캐시 flush 불가 → 간이)
    total = 0
    for _ in range(10):
        cur = mcoll.find({}, {"_id":0, "symb":1, "timestamp":1, "close":1}).batch_size(5000).limit(500000)
        for _ in cur:
            total += 32
            if total >= bytes_target:
                return

def cache_bust_pg(pc, table, rows_target=500000):
    pc.execute(f'SELECT symb, "timestamp", close FROM {table} ORDER BY "timestamp" DESC LIMIT %s', (rows_target,))
    _ = pc.fetchall()

# ---------- PG 커넥션 유틸 ----------

def _prep_conn(conn):
    try:
        if not conn.autocommit:
            conn.autocommit = True  # 짧은 단건 쿼리 오버헤드 감소
    except Exception:
        pass
    return conn

# ---------- 쿼리 실행 (단건) ----------
# 각 함수는 pc(커서)를 선택적으로 받아 재사용할 수 있게 한다.

def run_latest_once(mcoll, pg_pool, table, symb, pc=None, cutoff_dt=None):
    # Mongo
    m_t0 = time.perf_counter()
    mq = {"symb": symb}
    if cutoff_dt is not None:
        mq["timestamp"] = {"$gte": cutoff_dt}
    mdoc = mcoll.find_one(mq, sort=[("timestamp", DESCENDING)], projection={"_id":0})
    m_t = time.perf_counter() - m_t0

    # Postgres
    own_conn = None
    need_close_pc = False
    if pc is None:
        own_conn = _prep_conn(pg_pool.getconn())
        pc = own_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        need_close_pc = True

    try:
        p_t0 = time.perf_counter()
        if cutoff_dt is None:
            pc.execute(
                f'''SELECT symb,"timestamp",open,high,low,close,volume
                      FROM {table}
                     WHERE symb=%s
                  ORDER BY "timestamp" DESC
                     LIMIT 1''',
                (symb,)
            )
        else:
            pc.execute(
                f'''SELECT symb,"timestamp",open,high,low,close,volume
                      FROM {table}
                     WHERE symb=%s AND "timestamp">=%s
                  ORDER BY "timestamp" DESC
                     LIMIT 1''',
                (symb, cutoff_dt)
            )
        prow = pc.fetchone()
        p_t = time.perf_counter() - p_t0
    finally:
        if need_close_pc:
            try: pc.close()
            except: pass
        if own_conn is not None:
            pg_pool.putconn(own_conn)

    return m_t, p_t, 1 if mdoc else 0, 1 if prow else 0

def run_range_once(mcoll, pg_pool, table, symb, start_dt, end_dt, sort_by_time=True, pc=None):
    # Mongo
    q = {"symb": symb, "timestamp": {"$gte": start_dt, "$lt": end_dt}}
    cur = mcoll.find(q, {"_id": 0})
    if sort_by_time:
        cur = cur.sort("timestamp", ASCENDING)
    m_t0 = time.perf_counter()
    mlist = list(cur)
    m_t = time.perf_counter() - m_t0

    # Postgres
    own_conn = None
    need_close_pc = False
    if pc is None:
        own_conn = _prep_conn(pg_pool.getconn())
        pc = own_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        need_close_pc = True

    try:
        sql = (f'''SELECT symb,"timestamp",open,high,low,close,volume
                     FROM {table}
                    WHERE symb=%s AND "timestamp">=%s AND "timestamp"<%s'''
               + (' ORDER BY "timestamp" ASC' if sort_by_time else ''))
        p_t0 = time.perf_counter()
        pc.execute(sql, (symb, start_dt, end_dt))
        plist = pc.fetchall()
        p_t = time.perf_counter() - p_t0
    finally:
        if need_close_pc:
            try: pc.close()
            except: pass
        if own_conn is not None:
            pg_pool.putconn(own_conn)

    return m_t, p_t, len(mlist), len(plist)

def run_agg_once(mcoll, pg_pool, table, symb, start_dt, end_dt, pc=None):
    # Mongo
    pipe = [
        {"$match": {"symb": symb, "timestamp": {"$gte": start_dt, "$lt": end_dt}}},
        {"$group": {"_id": {"$dateTrunc": {"date": "$timestamp", "unit": "day"}},
                    "avgClose": {"$avg": "$close"}, "sumVol": {"$sum": "$volume"}}},
        {"$sort": {"_id": 1}}
    ]
    m_t0 = time.perf_counter()
    mlist = list(mcoll.aggregate(pipe, allowDiskUse=True))
    m_t = time.perf_counter() - m_t0

    # Postgres
    own_conn = None
    need_close_pc = False
    if pc is None:
        own_conn = _prep_conn(pg_pool.getconn())
        pc = own_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        need_close_pc = True

    try:
        p_t0 = time.perf_counter()
        pc.execute(
            f'''SELECT date_trunc('day',"timestamp") AS day,
                       AVG(close) AS avg_close, SUM(volume) AS sum_vol
                  FROM {table}
                 WHERE symb=%s AND "timestamp">=%s AND "timestamp"<%s
              GROUP BY 1
              ORDER BY 1''',
            (symb, start_dt, end_dt)
        )
        plist = pc.fetchall()
        p_t = time.perf_counter() - p_t0
    finally:
        if need_close_pc:
            try: pc.close()
            except: pass
        if own_conn is not None:
            pg_pool.putconn(own_conn)

    return m_t, p_t, len(mlist), len(plist)

# ---------- 벤치 러너 ----------
# 동시성 1일 때는 커넥션/커서를 한 번만 만들고 전체 루프에서 재사용
# 동시성 >1일 때는 기존 방식(작업마다 빌리고 반납)

def run_bench(func, symbols, iters, concurrency, pg_pool=None):
    m_times, p_times, m_rows, p_rows, m_err, p_err = [], [], [], [], 0, 0

    if concurrency <= 1 and pg_pool is not None:
        conn = _prep_conn(pg_pool.getconn())
        try:
            pc = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            for _ in range(iters):
                for s in symbols:
                    try:
                        m_t, p_t, mr, pr = func(s, pc=pc)  # 재사용 커서
                        m_times.append(m_t); p_times.append(p_t)
                        m_rows.append(mr); p_rows.append(pr)
                    except Exception as e:
                        print("ERR:", type(e).__name__, e, file=sys.stderr)
        finally:
            try: pc.close()
            except: pass
            pg_pool.putconn(conn)
    else:
        for _ in range(iters):
            with ThreadPoolExecutor(max_workers=concurrency) as ex:
                futures = {ex.submit(func, s): s for s in symbols}
                for f in as_completed(futures):
                    try:
                        m_t, p_t, mr, pr = f.result()
                        m_times.append(m_t); p_times.append(p_t)
                        m_rows.append(mr); p_rows.append(pr)
                    except Exception as e:
                        print("ERR:", type(e).__name__, e, file=sys.stderr)

    return m_times, p_times, m_rows, p_rows, m_err, p_err

# ---------- EXPLAIN 샘플 ----------

def explain_samples(mcoll, pc, table, symb, start_dt, end_dt, latest_cutoff=None):
    print("\n--- EXPLAIN Samples ---")
    # Mongo: 최신 1건
    print("[Mongo] latest 1")
    mq = {"symb": symb}
    if latest_cutoff is not None:
        mq["timestamp"] = {"$gte": latest_cutoff}
    print(mcoll.find(mq).sort("timestamp", -1).limit(1).explain("executionStats"))
    # Mongo: 범위
    print("[Mongo] range asc")
    print(mcoll.find({"symb": symb, "timestamp":{"$gte":start_dt,"$lt":end_dt}})
              .sort("timestamp", 1).limit(5).explain("executionStats"))
    # PG: 최신 1건
    print("[PG] latest 1")
    if latest_cutoff is None:
        pc.execute(
            f'EXPLAIN (ANALYZE, BUFFERS) SELECT symb,"timestamp",open,high,low,close,volume FROM {table} '
            f'WHERE symb=%s ORDER BY "timestamp" DESC LIMIT 1', (symb,))
    else:
        pc.execute(
            f'EXPLAIN (ANALYZE, BUFFERS) SELECT symb,"timestamp",open,high,low,close,volume FROM {table} '
            f'WHERE symb=%s AND "timestamp">=%s ORDER BY "timestamp" DESC LIMIT 1', (symb, latest_cutoff))
    print("\n".join(r[0] for r in pc.fetchall()))
    # PG: 범위 asc
    print("[PG] range asc")
    pc.execute(
        f'EXPLAIN (ANALYZE, BUFFERS) SELECT symb,"timestamp",open,high,low,close,volume FROM {table} '
        f'WHERE symb=%s AND "timestamp">=%s AND "timestamp"<%s ORDER BY "timestamp" ASC',
        (symb, start_dt, end_dt))
    print("\n".join(r[0] for r in pc.fetchall()))

# ---------- 메인 ----------

def main():
    ap = argparse.ArgumentParser("MongoDB vs PostgreSQL(Timescale) Benchmark (enhanced)")
    ap.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://localhost:27017/trader"),
    )
    ap.add_argument("--mongo-db",  default=os.getenv("MONGO_DB","trader"))
    ap.add_argument("--mongo-col", default=os.getenv("MONGO_COL","stock_ts"))

    # 로컬 테스트에 유리한 기본 DSN (필요시 환경변수 PG_DSN으로 덮어쓰기)
    ap.add_argument("--pg-dsn", default=os.getenv(
        "PG_DSN",
        "hostaddr=127.0.0.1 port=5432 dbname=trader user=postgres "
        "sslmode=disable gssencmode=disable "
        "options='-c timezone=UTC -c jit=off -c client_encoding=UTF8 "
        "-c timescaledb.enable_chunk_append=on -c timescaledb.enable_optimized_order_by=on'"
    ))
    ap.add_argument("--pg-table",  default=os.getenv("PG_TABLE","public.stock"))

    ap.add_argument("--symbols",   default="", help="쉼표구분 목록, 비우면 DB에서 추출")
    ap.add_argument("--num-symbols", type=int, default=10)
    ap.add_argument("--dist", choices=["uniform","zipf"], default="uniform")
    ap.add_argument("--zipf-alpha", type=float, default=1.1)

    ap.add_argument("--start", default="2020-01-01")
    ap.add_argument("--days", type=int, default=30)

    ap.add_argument("--iters", type=int, default=3)
    ap.add_argument("--concurrency", type=int, default=1)

    ap.add_argument("--warm-runs", type=int, default=1, help="웜 상태 반복 횟수(요약은 합산)")
    ap.add_argument("--do-cold", action="store_true", help="간이 캐시 교란 후 Cold 측정도 수행")
    ap.add_argument("--cold-bytes", type=int, default=64*1024*1024)

    ap.add_argument("--explain", action="store_true", help="샘플 EXPLAIN 출력")
    ap.add_argument("--seed", type=int, default=42)

    ap.add_argument("--mode", choices=["latest","range","agg","all"], default="all")

    # ★ 최신 1건 플래닝 폭탄 방지용 시간 하한(일)
    ap.add_argument("--latest-horizon-days", type=int,
                    default=int(os.getenv("LATEST_HORIZON_DAYS","90")),
                    help="최신 1건 조회 시 시간 하한(일). Mongo/PG 모두 동일하게 적용.")

    args = ap.parse_args()

    random.seed(args.seed)

    start_dt = ensure_utc_iso(args.start)
    end_dt   = start_dt + timedelta(days=args.days)

    # 최신 1건 하한 컷오프(UTC now 기준)
    latest_cutoff_utc = datetime.now(timezone.utc) - timedelta(days=args.latest_horizon_days)

    # Connect
    mcli = MongoClient(args.mongo_uri, uuidRepresentation="standard")
    mdb = mcli[args.mongo_db]; mcoll = mdb[args.mongo_col]

    # Postgres Threaded Pool
    pg_pool = ThreadedConnectionPool(
        minconn=max(1, min(2, args.concurrency)),
        maxconn=max(4, args.concurrency*2),
        dsn=args.pg_dsn
    )

    # 하나의 관리용 커넥션(메타/EXPLAIN/버스터 용)
    admin_conn = _prep_conn(pg_pool.getconn())
    admin_pc = admin_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # 심볼 수집
    if args.symbols.strip():
        syms = [s.strip() for s in args.symbols.split(",") if s.strip()]
    else:
        syms = list(mcoll.distinct("symb"))
        if not syms:
            print("Mongo에서 symb를 찾지 못했습니다.", file=sys.stderr)
            sys.exit(1)
        if args.dist == "uniform":
            syms = pick_symbols_uniform(syms, args.num_symbols)
        else:
            syms = pick_symbols_zipf(syms, args.num_symbols, alpha=args.zipf_alpha)

    print("Symbols:", syms)
    print(f"Mode={args.mode}, Concurrency={args.concurrency}, Dist={args.dist} (alpha={args.zipf_alpha})")
    print(f"Range: {start_dt.isoformat()} ~ {end_dt.isoformat()}  (days={args.days})")
    print(f"Latest horizon: now- {args.latest_horizon_days} days (UTC cutoff={latest_cutoff_utc.isoformat()})")

    # 간단 웜업 (커넥션/드라이버 준비)
    _ = list(mcoll.find({"symb": syms[0]}).limit(1))
    admin_pc.execute(f'SELECT 1 FROM {args.pg_table} WHERE symb=%s LIMIT 1', (syms[0],)); _ = admin_pc.fetchone()

    def meta_tag(tag):
        return dict(mode=tag, conc=args.concurrency, dist=args.dist, iters=args.iters, symbols=len(syms))

    # EXPLAIN (옵션)
    if args.explain:
        explain_samples(mcoll, admin_pc, args.pg_table, syms[0], start_dt, end_dt, latest_cutoff=latest_cutoff_utc)

    # 러너 바인딩 (pc 재사용 가능한 래퍼)
    def f_latest(symb, pc=None):
        return run_latest_once(mcoll, pg_pool, args.pg_table, symb, pc=pc, cutoff_dt=latest_cutoff_utc)
    def f_range(symb, pc=None):
        return run_range_once(mcoll, pg_pool, args.pg_table, symb, start_dt, end_dt, sort_by_time=True, pc=pc)
    def f_agg(symb, pc=None):
        return run_agg_once(mcoll, pg_pool, args.pg_table, symb, start_dt, end_dt, pc=pc)

    # Cold 측정 (옵션)
    if args.do_cold:
        print("\n[COLD] 캐시 교란 중...")
        cache_bust_mongo(mcoll, bytes_target=args.cold_bytes)
        cache_bust_pg(admin_pc, args.pg_table, rows_target=500000)

        if args.mode in ("latest","all"):
            m,p, mr, pr, *_ = run_bench(f_latest, syms, args.iters, args.concurrency, pg_pool=pg_pool)
            summarize("A) 최신 1건 (COLD)", m,p, mr, pr, meta=meta_tag("latest-cold"))

        if args.mode in ("range","all"):
            m,p, mr, pr, *_ = run_bench(f_range, syms, args.iters, args.concurrency, pg_pool=pg_pool)
            summarize(f"B) 기간 조회 {args.days}일 (정렬 포함, COLD)", m,p, mr, pr, meta=meta_tag("range-cold"))

        if args.mode in ("agg","all"):
            m,p, mr, pr, *_ = run_bench(f_agg, syms, args.iters, args.concurrency, pg_pool=pg_pool)
            summarize(f"C) 일별 집계 {args.days}일 (COLD)", m,p, mr, pr, meta=meta_tag("agg-cold"))

    # Warm 측정 (여러 라운드 합산)
    for r in range(args.warm_runs):
        if args.mode in ("latest","all"):
            m,p, mr, pr, *_ = run_bench(f_latest, syms, args.iters, args.concurrency, pg_pool=pg_pool)
            summarize(f"A) 최신 1건 (WARM round {r+1}/{args.warm_runs})", m,p, mr, pr, meta=meta_tag("latest-warm"))

        if args.mode in ("range","all"):
            m,p, mr, pr, *_ = run_bench(f_range, syms, args.iters, args.concurrency, pg_pool=pg_pool)
            summarize(f"B) 기간 조회 {args.days}일 (정렬 포함, WARM round {r+1}/{args.warm_runs})", m,p, mr, pr, meta=meta_tag("range-warm"))

        if args.mode in ("agg","all"):
            m,p, mr, pr, *_ = run_bench(f_agg, syms, args.iters, args.concurrency, pg_pool=pg_pool)
            summarize(f"C) 일별 집계 {args.days}일 (WARM round {r+1}/{args.warm_runs})", m,p, mr, pr, meta=meta_tag("agg-warm"))

    # 종료
    admin_pc.close()
    pg_pool.putconn(admin_conn)
    pg_pool.closeall()
    mcli.close()

if __name__ == "__main__":
    main()
