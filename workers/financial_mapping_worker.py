"""Consumes only stored-fact mapping jobs, never external collection."""
import argparse,json,logging,signal,threading,time
from datetime import date
from pipeline.financial_mapping667 import apply_company
from workers.db import connect_database

LOG=logging.getLogger('financial-mapping-worker')
def process_one(connection):
 try:
  with connection.cursor() as cur:
   cur.execute("SET LOCAL statement_timeout='15s'");cur.execute("SET LOCAL lock_timeout='3s'")
   cur.execute("""SELECT company_id,generation,start_date,end_date,annual_only,refresh_requested,reason FROM finance_mapping667.queue
    WHERE status='PENDING' ORDER BY requested_at,company_id FOR UPDATE SKIP LOCKED LIMIT 1""")
   row=cur.fetchone()
   if not row:connection.rollback();return None
   company,generation,start,end,annual_only,refresh_requested,reason=row
   try:
    if reason=='REVIEWED_RECONCILIATION677':
     from pipeline.financial_reconciliation677 import apply_reviewed_company
     result=apply_reviewed_company(cur,company,start,end or date.today(),annual_only=annual_only)
    else:
     result=apply_company(cur,company,start,end or date.today(),annual_only=annual_only,repair_only=reason=='SEC_RAW_REPAIR674')
   except ValueError as exc:
    if str(exc) not in {'COMPANY_FACT_BUDGET_EXCEEDED','MAPPING_COMPANY_IDENTITY_UNVERIFIED'}:raise
    from pipeline.financial_mapping667 import canonical,digest,POLICY_VERSION
    reason='HELD_COMPANY_BUDGET' if str(exc)=='COMPANY_FACT_BUDGET_EXCEEDED' else 'HELD_COMPANY_IDENTITY'
    summary={'results':{reason:1}}
    cur.execute('INSERT INTO finance_mapping667.receipt(company_id,policy_version,start_date,end_date,summary,input_hash) VALUES(%s,%s,%s,%s,%s::jsonb,%s) RETURNING id',(company,POLICY_VERSION,start,end or date.today(),canonical(summary),digest(summary)))
    result={'companyId':company,'receiptId':cur.fetchone()[0],'results':summary['results']}
   cur.execute('UPDATE finance_mapping667.receipt SET queue_generation=%s WHERE id=%s',(generation,result['receiptId']))
   outcome='HELD' if any(k in result['results'] for k in ('HELD_COMPANY_BUDGET','HELD_COMPANY_IDENTITY')) else 'DONE'
   cur.execute("""UPDATE finance_mapping667.queue SET status=%s,processed_at=now(),receipt_id=%s,
    generation=generation+CASE WHEN refresh_requested THEN 1 ELSE 0 END,
    reason=CASE WHEN refresh_requested THEN 'SEC_ETL_DEFERRED' ELSE reason END,
    requested_at=CASE WHEN refresh_requested THEN now() ELSE requested_at END,
    start_date=CASE WHEN refresh_requested THEN DATE '1900-01-01' ELSE start_date END,
    end_date=CASE WHEN refresh_requested THEN NULL ELSE end_date END,
    annual_only=CASE WHEN refresh_requested THEN FALSE ELSE annual_only END,refresh_requested=FALSE
    WHERE company_id=%s AND generation=%s""",('PENDING' if refresh_requested else outcome,result['receiptId'],company,generation))
  connection.commit()
  # New read after commit validates the receipt and queue together.
  with connection.cursor() as cur:
   cur.execute('SELECT 1 FROM finance_mapping667.receipt r WHERE r.id=%s AND r.company_id=%s AND r.queue_generation=%s',(result['receiptId'],company,generation))
   if cur.fetchone()!=(1,):raise RuntimeError('MAPPING_COMMIT_UNVERIFIED')
  connection.rollback();return result
 except BaseException:
  connection.rollback();raise

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--loop',action='store_true');parser.add_argument('--max-companies',type=int,default=0);parser.add_argument('--interval',type=float,default=2)
 args=parser.parse_args();stop=threading.Event()
 for signum in (signal.SIGTERM,signal.SIGINT):signal.signal(signum,lambda *_:stop.set())
 count=0;connection=connect_database()
 try:
  while not stop.is_set():
   result=process_one(connection)
   if result:
    count+=1;LOG.warning(json.dumps({'companyId':result['companyId'],'receiptId':result['receiptId'],'results':result['results']}))
   if args.max_companies and count>=args.max_companies:break
   if not result and not args.loop:break
   stop.wait(max(0,args.interval) if result else max(2,args.interval))
 finally:connection.close()
if __name__=='__main__':main()
