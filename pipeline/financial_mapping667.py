"""Deterministic, date-scoped serving approvals; never mutate financial facts."""
import hashlib,json,re
from collections import Counter,defaultdict
from datetime import date
from decimal import Decimal,InvalidOperation
from urllib.parse import urlsplit

POLICY_VERSION='sec-serving-mapping-v1'
RULES={
 'REVENUE':('USD',{'Revenues','RevenueFromContractWithCustomerExcludingAssessedTax'}),
 'NET_INCOME':('USD',{'NetIncomeLoss'}),
 'OPERATING_INCOME':('USD',{'OperatingIncomeLoss'}),
 'ASSETS':('USD',{'Assets'}),'LIABILITIES':('USD',{'Liabilities'}),
 'DILUTED_EPS':('USD/shares',{'EarningsPerShareDiluted'})}

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),default=str)
def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()
def day(value):return date.fromisoformat(str(value)) if value is not None else None

def invalid(row,*,today=None,annual_only=False):
 today=today or date.today();code=row.get('metric_code')
 if code not in RULES:return 'UNSUPPORTED_METRIC'
 unit,tags=RULES[code]
 if row.get('taxonomy')!='us-gaap' or row.get('source_tag') not in tags:return 'UNREVIEWED_TAG'
 if row.get('unit')!=unit or row.get('dimensions') not in (None,{}):return 'UNIT_OR_DIMENSIONS'
 try:
  end=day(row['period_end']);start=day(row.get('period_start'));filed=day(row['filing_date'])
  value=Decimal(str(row['value']))
  if not value.is_finite() or end>today or filed>today or filed<end:return 'INVALID_VALUE_OR_DATE'
  form=row['form_type'];base=form.removesuffix('/A')
  if base not in {'10-K','10-Q'}:return 'UNREVIEWED_FORM'
  if day(row['report_date'])!=end:return 'COMPARATIVE_PERIOD'
  if code in {'ASSETS','LIABILITIES'}:
   if start is not None:return 'INSTANT_START_PRESENT'
  elif start is None or not (70<=(end-start).days<=105 or 335<=(end-start).days<=395):return 'UNSUPPORTED_DURATION'
  if annual_only and (base!='10-K' or start is not None and not 335<=(end-start).days<=395):return 'NON_ANNUAL_SCOPE'
  cik=str(int(row['cik']));accession=row['accession_number'];url=urlsplit(row['source_url'])
  if not re.fullmatch(r'\d{10}-\d{2}-\d{6}',accession):return 'INVALID_ACCESSION'
  if url.scheme!='https' or url.netloc!='www.sec.gov' or not url.path.startswith('/Archives/edgar/data/'+cik+'/'+accession.replace('-','')+'/') or not re.fullmatch(r'[A-Za-z0-9/_.-]+',url.path) or '..' in url.path or url.query or url.fragment:return 'INVALID_SOURCE_URL'
  if not re.fullmatch(r'[a-f0-9]{64}',row.get('source_fact_hash') or ''):return 'SOURCE_HASH_MISSING'
 except (ValueError,TypeError,KeyError,InvalidOperation):return 'INVALID_SOURCE_METADATA'
 return None

def proposals(rows,mappings,*,today=None,annual_only=False):
 groups=defaultdict(list);excluded=Counter()
 for row in rows:
  reason=invalid(row,today=today,annual_only=annual_only)
  if reason:excluded[reason]+=1;continue
  groups[(int(row['company_id']),int(row['metric_id']),str(row['period_end']))].append(row)
 decisions=[]
 for (company,metric,end),facts in sorted(groups.items()):
  from pipeline.known_corrections712 import blocked_row,REASON as LATE_REASON
  scoped=[m for m in mappings if m['metric_id']==metric and m.get('company_id') in (None,company) and (not m.get('valid_from') or str(m['valid_from'])<=end) and (not m.get('valid_to') or str(m['valid_to'])>=end)]
  effective=[m for m in scoped if m.get('company_id')==company] or scoped
  approved={(m['taxonomy'],m['source_tag']) for m in effective}
  available={(f['taxonomy'],f['source_tag']) for f in facts}
  reason=LATE_REASON if any(blocked_row(f) for f in facts) else None;tag=None
  if len(approved)>1:reason='EXISTING_MAPPING_CONFLICT'
  elif approved:
   if not approved<=available:reason='EXISTING_TAG_WITHOUT_VERIFIED_FACT'
   else:tag=next(iter(approved))[1]
  elif len(available)!=1:reason='SOURCE_TAG_CONFLICT'
  else:tag=next(iter(available))[1]
  if approved and len(available)>1:
   by_start=defaultdict(list)
   for f in facts:by_start[str(f.get('period_start'))].append(f)
   for entries in by_start.values():
    newest=max(str(f['filing_date']) for f in entries)
    if len({Decimal(str(f['value'])) for f in entries if str(f['filing_date'])==newest})>1:reason='ALIAS_VALUE_CONFLICT'
  selected=[]
  if tag:
   starts=defaultdict(list)
   for f in facts:
    if f['source_tag']==tag:starts[str(f.get('period_start'))].append(f)
   for entries in starts.values():
    newest=max(str(f['filing_date']) for f in entries);latest=[f for f in entries if str(f['filing_date'])==newest]
    if len({Decimal(str(f['value'])) for f in latest})!=1:reason='LATEST_FILING_VALUE_CONFLICT';break
    selected.extend(latest)
  if len(facts)>128:reason='SOURCE_SET_BUDGET'
  proof={'candidateHash':digest(sorted((str(f['fact_id']),f['source_fact_hash'],f['source_tag']) for f in facts)),'candidates':sorted([{'factId':f['fact_id'],'hash':f['source_fact_hash'],'tag':f['source_tag'],'accession':f['accession_number']} for f in facts],key=lambda x:(str(x['factId']),x['hash']))[:128],'policy':POLICY_VERSION,'companyId':company,'metricId':metric,'metricCode':facts[0]['metric_code'],'end':end,'tag':tag,'sources':sorted([{'factId':f['fact_id'],'hash':f['source_fact_hash'],'accession':f['accession_number'],'filed':str(f['filing_date']),'start':str(f.get('period_start')),'end':str(f['period_end']),'value':str(f['value']),'unit':f['unit'],'url':f['source_url']} for f in selected[:128]],key=lambda x:(x['hash'],x['accession']))}
  decisions.append({'company_id':company,'metric_id':metric,'metric_code':facts[0]['metric_code'],'end':end,'source_tag':tag,'outcome':'HELD' if reason else 'EXISTING' if approved else 'APPROVE','reason':reason,'proof':proof,'proof_hash':digest(proof)})
 return decisions,dict(excluded)

def enqueue(cur,company_id,reason='SEC_ETL'):
 cur.execute("""INSERT INTO finance_mapping667.queue(company_id,status,reason) VALUES(%s,'PENDING',%s)
 ON CONFLICT(company_id) DO UPDATE SET
 refresh_requested=(finance_mapping667.queue.status='PENDING'),
 status='PENDING',reason=CASE WHEN finance_mapping667.queue.status='PENDING' THEN finance_mapping667.queue.reason ELSE EXCLUDED.reason END,
 requested_at=CASE WHEN finance_mapping667.queue.status='PENDING' THEN finance_mapping667.queue.requested_at ELSE now() END,
 generation=finance_mapping667.queue.generation+CASE WHEN finance_mapping667.queue.status='PENDING' THEN 0 ELSE 1 END,
 start_date=CASE WHEN finance_mapping667.queue.status='PENDING' THEN finance_mapping667.queue.start_date ELSE DATE '1900-01-01' END,
 end_date=CASE WHEN finance_mapping667.queue.status='PENDING' THEN finance_mapping667.queue.end_date ELSE NULL END,
 annual_only=CASE WHEN finance_mapping667.queue.status='PENDING' THEN finance_mapping667.queue.annual_only ELSE FALSE END""",(int(company_id),reason))

def enqueue_if_enabled(cur,company_id):
 import os
 if os.getenv('FINANCIAL_MAPPING_QUEUE_ENABLED','false').lower()!='true':return
 enqueue(cur,company_id)

FACT_SQL="""SELECT f.id AS fact_id,f.company_id,f.metric_id,m.metric_code,f.taxonomy,f.source_tag,f.unit,
 f.period_start,f.period_end,f.value,f.dimensions,f.source_fact_hash,g.accession_number,g.filing_date,
 g.report_date,g.document_url AS source_url,g.form_type,c.cik
 FROM public.financial_fact f JOIN public.financial_metric m ON m.id=f.metric_id
 JOIN public.filing g ON g.id=f.filing_id AND g.company_id=f.company_id
 JOIN public.company c ON c.id=f.company_id
 WHERE f.company_id=%s AND m.metric_code=ANY(%s) AND f.period_end BETWEEN %s AND %s
 AND f.taxonomy='us-gaap' AND (f.dimensions IS NULL OR f.dimensions='{}'::jsonb)
 ORDER BY f.metric_id,f.period_end,f.id LIMIT 10001"""


COLLECTION_FACT_SQL="""SELECT 'control649:'||f.id::text AS fact_id,f.company_id,pm.id AS metric_id,
 pm.metric_code,f.taxonomy,f.source_tag,f.unit,f.period_start,f.period_end,f.value,f.dimensions,
 f.source_fact_hash,g.accession_number,g.filing_date,g.report_date,g.document_url AS source_url,
 g.form_type,c.cik
 FROM control649.financial_fact f JOIN control649.financial_metric sm ON sm.id=f.metric_id
 JOIN public.financial_metric pm ON pm.metric_code=sm.metric_code
 JOIN control649.filing g ON g.id=f.filing_id AND g.company_id=f.company_id
 JOIN control649.company c ON c.id=f.company_id
 JOIN public.company canonical ON canonical.id=c.id AND canonical.cik=c.cik
 WHERE f.company_id=%s AND pm.metric_code=ANY(%s) AND f.period_end BETWEEN %s AND %s
 AND f.taxonomy='us-gaap' AND (f.dimensions IS NULL OR f.dimensions='{}'::jsonb)
 ORDER BY pm.id,f.period_end,f.id LIMIT 10001"""

def read_company(cur,company_id,start,end,*,annual_only=False):
 cur.execute(FACT_SQL,(company_id,list(RULES),start,end))
 rows=[dict(zip([d[0] for d in cur.description],r)) for r in cur.fetchall()]
 # Shared collection uses isolated writable tables. Match canonical identities
 # and metric codes before approving tags; do not publish staging facts here.
 cur.execute("SELECT s.cik,p.cik FROM control649.company s LEFT JOIN public.company p ON p.id=s.id WHERE s.id=%s",(company_id,))
 identity=cur.fetchone()
 if not identity or not all(identity) or str(identity[0])!=str(identity[1]):
  raise ValueError('MAPPING_COMPANY_IDENTITY_UNVERIFIED')
 cur.execute(COLLECTION_FACT_SQL,(company_id,list(RULES),start,end))
 rows.extend(dict(zip([d[0] for d in cur.description],r)) for r in cur.fetchall())
 if len(rows)>10000:raise ValueError('COMPANY_FACT_BUDGET_EXCEEDED')
 cur.execute('SELECT metric_id,company_id,taxonomy,source_tag,valid_from,valid_to FROM public.financial_metric_mapping WHERE company_id=%s OR company_id IS NULL',(company_id,))
 mappings=[dict(zip([d[0] for d in cur.description],r)) for r in cur.fetchall()]
 return proposals(rows,mappings,annual_only=annual_only)

def apply_company(cur,company_id,start,end,*,annual_only=False,repair_only=False):
 cur.execute("SELECT pg_advisory_xact_lock(hashtextextended('finance_mapping667:'||%s::text,0))",(str(company_id),))
 decisions,excluded=read_company(cur,company_id,start,end,annual_only=annual_only)
 from pipeline.financial_history674 import merge_verified_history
 decisions=merge_verified_history(cur,company_id,start,end,annual_only,decisions,repair_only=repair_only)
 result=Counter()
 for d in decisions:
  proof=canonical(d['proof']);outcome=d['outcome']
  if outcome=='APPROVE':
   cur.execute("""INSERT INTO finance_mapping667.approved(company_id,metric_id,taxonomy,source_tag,priority,valid_from,valid_to,policy_version,proof,proof_hash)
    VALUES(%s,%s,'us-gaap',%s,100,%s,%s,%s,%s::jsonb,%s) ON CONFLICT(company_id,metric_id,valid_from,valid_to) DO UPDATE SET source_tag=EXCLUDED.source_tag,active=TRUE,proof=EXCLUDED.proof,proof_hash=EXCLUDED.proof_hash,policy_version=EXCLUDED.policy_version WHERE NOT approved.active RETURNING id""",(company_id,d['metric_id'],d['source_tag'],d['end'],d['end'],d.get('policy_version',POLICY_VERSION),proof,d['proof_hash']))
   outcome='INSERTED' if cur.fetchone() else 'EXISTING'
  if outcome=='HELD':
   cur.execute('UPDATE finance_mapping667.approved SET active=FALSE WHERE company_id=%s AND metric_id=%s AND valid_from=%s AND valid_to=%s AND active',(company_id,d['metric_id'],d['end'],d['end']))
   cur.execute("""INSERT INTO finance_mapping667.held(company_id,metric_id,period_end,reason,proof_hash,proof) VALUES(%s,%s,%s,%s,%s,%s::jsonb)
    ON CONFLICT(company_id,metric_id,period_end,reason,proof_hash) DO NOTHING""",(company_id,d['metric_id'],d['end'],d['reason'],d['proof_hash'],proof))
  result[outcome]+=1
 cur.execute("""INSERT INTO finance_mapping667.receipt(company_id,policy_version,start_date,end_date,summary,input_hash)
 VALUES(%s,%s,%s,%s,%s::jsonb,%s) RETURNING id""",(company_id,POLICY_VERSION,start,end,canonical({'results':dict(result),'excluded':excluded}),digest(decisions)))
 receipt=cur.fetchone()[0]
 return {'companyId':company_id,'receiptId':receipt,'results':dict(result),'excluded':excluded}
