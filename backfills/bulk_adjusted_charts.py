"""로컬 기존 일봉 보유 종목을 작은 세대로 수집·검증·활성화한다.

전체를 하나의 6시간 세대로 섞지 않는다. 종목별 오류는 보류하고 성공한
종목만 기존 게시 검증을 통과한 뒤 기본 차트로 전환한다. 원본 가격 불변.
"""
import argparse
import contextlib
import json
import math
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extensions import parse_dsn
from dotenv import load_dotenv
from backfills.backfill_adjusted_charts import main as collect, save
from backfills.activate_adjusted_charts import main as activate
from collectors.kis_client import KISClient
from collectors.kis_diagnostics import error_metadata


class BackfillKISClient(KISClient):
    """Stop acceleration immediately after an error, without retrying that request."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Publishing/validation can leave a pooled socket idle. KIS can close
        # it before the next asset request; use a fresh connection per request.
        # Keep the session's authentication, pacing and retry policy unchanged.
        self.session.headers['Connection'] = 'close'

    def _slow_down(self, error):
        self.min_request_interval = max(1, self.min_request_interval)
        metadata = error_metadata(error)
        if metadata['failureCategory'] == 'RATE_LIMIT':
            # Honor numeric Retry-After; never immediately retry the failed page.
            self.last_request_at = max(self.last_request_at, time.monotonic() +
                                       max(60, metadata['retryAfterSeconds'] or 0))

    def _request(self, *args, **kwargs):
        try:
            return super()._request(*args, **kwargs)
        except Exception as error:
            self._slow_down(error)
            raise

    def get_daily_price_page(self, *args, **kwargs):
        try:
            return super().get_daily_price_page(*args, **kwargs)
        except Exception as error:
            self._slow_down(error)
            raise


def request_interval(value):
    """Keep the serial collector at no more than two requests per second."""
    result = float(value)
    if not math.isfinite(result) or not 0.5 <= result <= 5:
        raise argparse.ArgumentTypeError('요청 간격은 0.5~5초여야 합니다.')
    return result


def slow_down_on_provider_error(client, assets):
    errors = {'KISAPIError', 'HTTPError', 'ReadTimeout', 'ConnectTimeout',
              'ConnectionError', 'RetryError', 'Timeout'}
    if client.min_request_interval < 1 and any(
        entry.get('errorType') in errors for entry in assets.values()
    ):
        client.min_request_interval = 1


def batches(targets, max_pages=250, max_assets=25):
    """행 수 추정으로 묶되 큰 단일 종목은 독립 배치로 둔다."""
    batch=[]; pages=0
    for target in targets:
        estimate=max(1,math.ceil(target['rows']/100))
        if batch and (pages+estimate>max_pages or len(batch)>=max_assets):
            yield batch
            batch=[]; pages=0
        batch.append(target); pages+=estimate
    if batch: yield batch


def plan(conn):
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout='120s'")
        cur.execute('''SELECT p.asset_id,s.provider_symbol,count(*),
            min((p.bar_time AT TIME ZONE 'UTC')::date),max((p.bar_time AT TIME ZONE 'UTC')::date)
            FROM price_bar p JOIN asset a ON a.id=p.asset_id
            JOIN asset_provider_symbol s ON s.asset_id=a.id AND s.provider='KIS' AND s.is_current
            WHERE p.source='KIS' AND p."interval"='1D' AND p.display_policy='FULL_OHLC'
              AND a.active AND a.currency='USD'
              AND NOT EXISTS(SELECT 1 FROM price_chart_snapshot q WHERE q.asset_id=a.id AND q.default_for_chart)
            GROUP BY p.asset_id,s.provider_symbol ORDER BY count(*) ASC,p.asset_id''')
        return [{'assetId':r[0],'symbol':r[1],'rows':r[2],'start':str(r[3]),'end':str(r[4])} for r in cur.fetchall()]


@contextlib.contextmanager
def exclusive_lock(path):
    """운영체제 파일 잠금: 비정상 종료 후에도 영구 잠금 파일을 남기지 않는다."""
    import msvcrt
    with path.open('a+b') as f:
        if f.tell()==0: f.write(b'0'); f.flush()
        f.seek(0)
        try: msvcrt.locking(f.fileno(),msvcrt.LK_NBLCK,1)
        except OSError: raise RuntimeError('수정주가 전체 수집이 이미 실행 중입니다.') from None
        try: yield
        finally:
            f.seek(0); msvcrt.locking(f.fileno(),msvcrt.LK_UNLCK,1)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--max-batches',type=int,default=10000)
    parser.add_argument('--request-interval',type=request_interval,default=1.0)
    args=parser.parse_args(argv)
    dsn=os.environ['LOCAL_PRICE_DATABASE_URL']
    if parse_dsn(dsn).get('host') not in {'localhost','127.0.0.1'}: raise ValueError('로컬 DB 전용입니다.')
    args.directory.mkdir(parents=True,exist_ok=True)
    with exclusive_lock(args.directory.parent/'adjusted-bulk.lock'):
        plan_file=args.directory/'plan.json'
        if plan_file.exists(): targets=json.loads(plan_file.read_text(encoding='utf-8'))['targets']
        else:
            with psycopg2.connect(dsn) as conn: targets=plan(conn)
            save(plan_file,{'createdAt':datetime.now(timezone.utc).isoformat(),'targets':targets})
        groups=list(batches(targets))
        summary={'targets':len(targets),'rows':sum(t['rows'] for t in targets),
                 'estimatedPages':sum(math.ceil(t['rows']/100) for t in targets),'batches':len(groups)}
        print(json.dumps(summary),flush=True)
        if not args.apply: return
        load_dotenv(Path(__file__).resolve().parents[1]/'.env',override=False)
        client=BackfillKISClient.from_env(timeout=20,min_request_interval=args.request_interval,max_retries=0)
        progress_file=args.directory/'progress.json'
        progress=json.loads(progress_file.read_text(encoding='utf-8')) if progress_file.exists() else {'batches':{},'status':'RUNNING'}
        started=time.monotonic(); processed=0; consecutive_failed=0
        try:
            for index,group in enumerate(groups):
                key=f'{index:05d}'
                if progress['batches'].get(key,{}).get('status')=='COMPLETE': continue
                if processed>=args.max_batches: break
                if (args.directory/'STOP').exists():
                    progress['status']='PAUSED_STOP_FILE'; break
                if shutil.disk_usage(args.directory).free < 10*1024**3:
                    progress['status']='PAUSED_LOW_DISK'; break
                # Only one worker; no retry loop for a bad provider/source row.
                symbols=[t['symbol'] for t in group]
                directory=args.directory/key
                progress.update(status='RUNNING',currentBatch=key,
                                requestIntervalSeconds=client.min_request_interval,
                                updatedAt=datetime.now(timezone.utc).isoformat())
                save(progress_file,progress)
                try: collect([str(directory),'--apply','--symbols',*symbols],shared_client=client)
                except SystemExit as e:
                    if e.code not in (0,1,None): raise
                state=json.loads((directory/'state.json').read_text(encoding='utf-8'))
                slow_down_on_provider_error(client, state['assets'])
                progress['requestIntervalSeconds']=client.min_request_interval
                successful=[s for s in symbols if state['assets'].get(s,{}).get('status')=='PUBLISHED']
                failed=[s for s in symbols if s not in successful]
                if successful:
                    activate([str(directory),'--apply','--symbols',*successful])
                    consecutive_failed=0
                else:
                    provider_failures={'KISAPIError','HTTPError','ReadTimeout','ConnectTimeout','ConnectionError','RetryError','Timeout'}
                    # Source quality rejection is not evidence of provider outage.
                    consecutive_failed=(consecutive_failed+1 if any(
                        state['assets'].get(s,{}).get('errorType') in provider_failures for s in failed) else 0)
                progress['batches'][key]={'status':'COMPLETE','activated':successful,'failed':failed}
                progress.update(elapsedSeconds=round(time.monotonic()-started),updatedAt=datetime.now(timezone.utc).isoformat())
                save(progress_file,progress); processed+=1
                # Whole-batch provider failures should not burn thousands of calls.
                if consecutive_failed>=3:
                    progress['status']='PAUSED_PROVIDER_FAILURES'; break
            else: progress['status']='COMPLETE_WITH_REVIEW' if any(b['failed'] for b in progress['batches'].values()) else 'COMPLETE'
            save(progress_file,progress)
            print(json.dumps({'status':progress['status'],'batchesDone':len(progress['batches']),
                              'activated':sum(len(b['activated']) for b in progress['batches'].values()),
                              'failed':sum(len(b['failed']) for b in progress['batches'].values())}),flush=True)
        except Exception as error:
            progress.update(status='FAILED_REVIEW_REQUIRED',errorType=type(error).__name__,updatedAt=datetime.now(timezone.utc).isoformat())
            save(progress_file,progress)
            raise
        finally: client.session.close()


if __name__=='__main__': main()
