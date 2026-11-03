"""Raw-verified historical mappings. No blanket alias or restatement guessing.

Only numerically unchanged original/comparative observations qualify. Changed
amounts require a separate reviewed accounting reconciliation (672 policy).
Evidence is append-only in the existing receipt table, consumed by the mapper.
"""
import os
import re
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal
from collectors.common import content_hash
from pipeline.financial_mapping667 import RULES, FACT_SQL, COLLECTION_FACT_SQL, canonical, digest, invalid

POLICY='sec-raw-unchanged-history674-v1'
EVIDENCE_POLICY='sec-raw-source-evidence674-v1'
FOREIGN_ANNUAL_FORMS=frozenset({'20-F','20-F/A','40-F','40-F/A'})
FRAME_POLICY='sec-optional-frame-history686-v1'
LEGACY_POLICY='sec-unchanged-legacy-net-sales693-v1'
HISTORY_POLICIES=frozenset({POLICY,FRAME_POLICY,LEGACY_POLICY})
KEYS=('fact_id','company_id','metric_id','metric_code','taxonomy','source_tag','unit',
      'period_start','period_end','value','dimensions','source_fact_hash',
      'accession_number','filing_date','report_date','source_url','form_type','cik')

def enabled():return os.getenv('FINANCIAL_HISTORY_RAW_ENABLED','false').lower()=='true'

def validate_proposal(d):
    if d.get('policy_version')==FRAME_POLICY:
        from pipeline.observation_frame686 import validate_proposal as validate_frame
        return validate_frame(d)
    p=d.get('proof',{});legacy=d.get('policy_version')==LEGACY_POLICY
    policy=LEGACY_POLICY if legacy else POLICY
    if (d.get('policy_version')!=policy or p.get('policy')!=policy or digest(p)!=d.get('proof_hash')
        or any(p.get(pk)!=d.get(dk) for pk,dk in [('companyId','company_id'),('metricId','metric_id'),('metricCode','metric_code'),('end','end'),('tag','source_tag')])
        or d.get('metric_code') not in RULES or (d.get('source_tag') not in RULES[d['metric_code']][1] and not (legacy and d.get('metric_code')=='REVENUE' and d.get('source_tag')=='SalesRevenueNet'))):
        raise ValueError('INVALID_PREPARED_EVIDENCE')
    if legacy and (d.get('metric_code')!='REVENUE' or d.get('source_tag')!='SalesRevenueNet' or p.get('servingTagContract')!='LEGACY_NET_SALES_SOURCE_TAG693_V1' or p.get('mappingScope')!='COMPANY_METRIC_PERIOD_END_ONLY' or not p.get('originals') or not p.get('sources') or not re.fullmatch('[a-f0-9]{64}',p.get('sourceScopeHash',''))):raise ValueError('INVALID_LEGACY_REVENUE_EVIDENCE')

def scope_hash(rows):
    stable=[]
    for row in rows:
        r={k:row.get(k) for k in KEYS}
        for k in ('fact_id','period_start','period_end','filing_date','report_date'):
            if r[k] is not None:r[k]=str(r[k])
        r['cik']=str(int(r['cik'])).zfill(10)
        amount=format(Decimal(str(r['value'])),'f')
        r['value']=amount.rstrip('0').rstrip('.') if '.' in amount else amount
        if Decimal(r['value'])==0:r['value']='0'
        stable.append(r)
    return digest(sorted(stable,key=lambda r:(r['fact_id'],r['source_fact_hash'] or '')))

def raw_index(payload,expected_cik):
    if str(int(payload['cik']))!=str(int(expected_cik)):raise ValueError('RAW_COMPANY_IDENTITY_MISMATCH')
    index={}
    for tag,concept in (payload.get('facts',{}).get('us-gaap',{})).items():
        if not any(tag in tags for _,tags in RULES.values()) and tag!='SalesRevenueNet':continue
        for unit,observations in (concept.get('units') or {}).items():
            for obs in observations:
                h=content_hash({'cik':str(int(expected_cik)).zfill(10),'taxonomy':'us-gaap',
                                'source_tag':tag,'unit':unit,'fact':obs})
                index[h]={'tag':tag,'unit':unit,'start':obs.get('start'),'end':obs.get('end'),
                          'value':str(obs.get('val')),'accession':obs.get('accn'),
                          'filed':obs.get('filed'),'form':obs.get('form')}
    return index

def unchanged_approval(rows,index,source,*,annual_only=False,today=None,foreign_annual=False,_legacy_serving=False):
    if not rows or len(rows)>128:raise ValueError('SOURCE_SCOPE_BUDGET')
    first=rows[0];code=first['metric_code'];cid=int(first['company_id']);mid=int(first['metric_id']);end=str(first['period_end'])
    if code not in RULES:raise ValueError('UNSUPPORTED_METRIC')
    if _legacy_serving and code!='REVENUE':raise ValueError('LEGACY_REVENUE_ONLY')
    unit,tags=RULES[code];valid=[];skipped=Counter()
    for row in rows:
        if (int(row['company_id']),int(row['metric_id']),row['metric_code'],str(row['period_end']),str(int(row['cik'])))!=(cid,mid,code,end,str(int(first['cik']))):
            raise ValueError('MIXED_SOURCE_IDENTITY')
        if row['taxonomy']!='us-gaap' or row['unit']!=unit or row['dimensions'] not in (None,{}):
            raise ValueError('UNREVIEWED_UNIT_TAXONOMY_OR_DIMENSIONS')
        tag=row['source_tag']
        if tag not in tags and not (code=='REVENUE' and tag=='SalesRevenueNet'):
            raise ValueError('UNREVIEWED_TAG')
        # Validate immutable metadata with the exact original validator, except
        # for the two explicitly reviewed dimensions: historical report date
        # and legacy revenue as an ORIGINAL comparison, never a serving alias.
        check=dict(row,report_date=row['period_end'])
        if tag=='SalesRevenueNet':check['source_tag']='Revenues'
        if foreign_annual and row['form_type'] in FOREIGN_ANNUAL_FORMS:
            # Validate an explicitly annual foreign report, never a 6-K or a
            # foreign quarter. The raw comparison below retains the REAL form.
            if code not in {'ASSETS','LIABILITIES'}:
                start=row.get('period_start')
                if start is None or not 335<=(date.fromisoformat(end)-date.fromisoformat(str(start))).days<=395:
                    skipped['FOREIGN_NON_ANNUAL_PERIOD']+=1;continue
            check['form_type']='10-K/A' if row['form_type'].endswith('/A') else '10-K'
        reason=invalid(check,today=today,annual_only=annual_only)
        if reason in {'UNREVIEWED_FORM','UNSUPPORTED_DURATION','NON_ANNUAL_SCOPE'}:
            skipped[reason]+=1;continue
        if reason:raise ValueError(reason)
        observation=index.get(row['source_fact_hash'])
        if not observation:raise ValueError('RAW_FACT_NOT_FOUND')
        expected={'tag':tag,'unit':unit,'start':str(row['period_start']) if row['period_start'] is not None else None,
                  'end':end,'value':str(row['value']),'accession':row['accession_number'],
                  'filed':str(row['filing_date']),'form':row['form_type']}
        if (Decimal(observation['value'])!=Decimal(expected['value'])
            or any(observation[k]!=expected[k] for k in expected if k!='value')):
            raise ValueError('RAW_OBSERVATION_MISMATCH')
        valid.append(row)
    if not valid:raise ValueError('NO_SUPPORTED_PERIOD')
    from pipeline.known_corrections712 import blocked_row,REASON as LATE_REASON
    if any(blocked_row(row) for row in valid):raise ValueError(LATE_REASON)
    starts=defaultdict(list)
    for row in valid:starts[str(row['period_start'])].append(row)
    serving_tags={'SalesRevenueNet'} if _legacy_serving else tags
    possible=set(serving_tags);selected=[];originals=[];comparatives=False
    for facts in starts.values():
        primary=[r for r in facts if str(r['report_date'])==end]
        if not primary:raise ValueError('ORIGINAL_REPORT_NOT_FOUND')
        # Any changed total, including a superseded original, needs explicit
        # accounting review. Equal latest values alone do not prove unchanged.
        if len({Decimal(str(r['value'])) for r in facts})!=1:
            raise ValueError('RESTATEMENT_REVIEW_REQUIRED')
        possible&={r['source_tag'] for r in facts if r['source_tag'] in serving_tags}
        originals.extend(primary)
        comparatives|=any(str(r['report_date'])!=end for r in facts)
    if not possible:raise ValueError('SUPPORTED_SUCCESSOR_TAG_NOT_FOUND')
    preferred='RevenueFromContractWithCustomerExcludingAssessedTax'
    tag=preferred if preferred in possible else sorted(possible)[0]
    for facts in starts.values():
        matching=[r for r in facts if r['source_tag']==tag]
        newest=max(str(r['filing_date']) for r in matching)
        selected.extend(r for r in matching if str(r['filing_date'])==newest)
    if not re.fullmatch('[0-9a-f]{64}',source.get('canonicalJsonSha256','')) or not source.get('objectUri'):
        raise ValueError('RAW_SOURCE_PROVENANCE_REQUIRED')
    for row in valid:
        if row['form_type'] not in FOREIGN_ANNUAL_FORMS:continue
        review=(source.get('foreignAnnualReview') or {}).get(row['accession_number'],{})
        if (review.get('basis')!='US_GAAP_PRIMARY_FINANCIAL_STATEMENTS'
            or review.get('cik')!=str(int(first['cik'])).zfill(10)
            or review.get('url')!=row['source_url']
            or not re.fullmatch('[0-9a-f]{64}',review.get('documentSha256',''))
            or not review.get('auditorOpinionExcerpt')):
            raise ValueError('FOREIGN_ANNUAL_BASIS_DOCUMENT_REVIEW_REQUIRED')
    selected_policy=LEGACY_POLICY if _legacy_serving else POLICY
    if _legacy_serving and any(r['form_type'] not in {'10-K','10-K/A','10-Q','10-Q/A'} for r in valid):raise ValueError('LEGACY_FOREIGN_SOURCE_REVIEW_REQUIRED')
    proof={'policy':selected_policy,'companyId':cid,'metricId':mid,'metricCode':code,'end':end,'tag':tag,
           'sourceScopeHash':scope_hash(rows),'rawSource':source,
           'basis':'UNCHANGED_ORIGINAL_AND_COMPARATIVE_SEC_XBRL' if comparatives else 'ORIGINAL_SEC_XBRL_CONFIRMED',
           'originals':[{'hash':r['source_fact_hash'],'accession':r['accession_number'],'value':str(r['value']),
                         'start':str(r['period_start']),'filed':str(r['filing_date'])} for r in originals],
           'sources':[{'hash':r['source_fact_hash'],'accession':r['accession_number'],'value':str(r['value']),
                       'start':str(r['period_start']),'filed':str(r['filing_date']),'url':r['source_url']} for r in selected],
           'rawMatchedFactHashes':sorted({r['source_fact_hash'] for r in valid}),
           'skipped':dict(skipped),'hashMeaning':'raw JSON canonical hash and individually matched SEC observation hashes; no inferred accounting adjustment'}
    if any(r['form_type'] in FOREIGN_ANNUAL_FORMS for r in valid):
        proof['annualFormContract']='SEC_FOREIGN_ANNUAL_US_GAAP675_V1'
        proof['validatedForms']=sorted({r['form_type'] for r in valid})
    if _legacy_serving:proof.update(servingTagContract='LEGACY_NET_SALES_SOURCE_TAG693_V1',mappingScope='COMPANY_METRIC_PERIOD_END_ONLY')
    return {'company_id':cid,'metric_id':mid,'metric_code':code,'end':end,'source_tag':tag,
            'outcome':'APPROVE','reason':None,'proof':proof,'proof_hash':digest(proof),'policy_version':selected_policy}


def legacy_revenue_approval(rows,index,source,**kwargs):
    """Use the real legacy tag only when every original comparison is unchanged.

    A supported modern common tag always keeps its existing policy. This does
    not rename raw tags, fill absent originals, resolve restatements or widen
    the ordinary 667 source whitelist.
    """
    try:unchanged_approval(rows,index,source,**kwargs)
    except ValueError as exc:
        if str(exc)!='SUPPORTED_SUCCESSOR_TAG_NOT_FOUND':raise
    else:raise ValueError('LEGACY_SERVING_NOT_NEEDED')
    return unchanged_approval(rows,index,source,_legacy_serving=True,**kwargs)


def retained_frame_map(cur,company,rows):
    """Partition-pruned reads. Never change a stored frame or source hash."""
    hashes=sorted({r['source_fact_hash'] for r in rows});result={}
    for table in ('public.financial_fact','control649.financial_fact'):
        cur.execute('SELECT source_fact_hash,frame FROM '+table+' WHERE company_id=%s AND source_fact_hash=ANY(%s) LIMIT 10001',(company,hashes))
        fetched=cur.fetchall()
        if len(fetched)>10000:raise ValueError('FRAME_METADATA_BUDGET')
        for h,frame in fetched:
            if h in result and result[h]!=frame:raise ValueError('FRAME_RETAINED_METADATA_CONFLICT')
            result[h]=frame
    return result

def read_rows(cur,company,start,end):
    cur.execute(FACT_SQL,(company,list(RULES),start,end));rows=[dict(zip([d[0] for d in cur.description],r)) for r in cur.fetchall()]
    cur.execute('SELECT s.cik,p.cik FROM control649.company s LEFT JOIN public.company p ON p.id=s.id WHERE s.id=%s',(company,))
    identity=cur.fetchone()
    if not identity or not all(identity) or str(int(identity[0]))!=str(int(identity[1])):raise ValueError('MAPPING_COMPANY_IDENTITY_UNVERIFIED')
    cur.execute(COLLECTION_FACT_SQL,(company,list(RULES),start,end));rows.extend(dict(zip([d[0] for d in cur.description],r)) for r in cur.fetchall())
    if len(rows)>10000:raise ValueError('COMPANY_FACT_BUDGET_EXCEEDED')
    return rows

def store_evidence(cur,company,proposals,excluded=None):
    for d in proposals:validate_proposal(d)
    if len(proposals)>1500 or any(d['company_id']!=company for d in proposals):
        raise ValueError('INVALID_PREPARED_EVIDENCE')
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended('finance_mapping667:'||%s::text,0))",(str(company),))
    summary={'results':{'RAW_VERIFIED_PROPOSALS':len(proposals)},'proposals674':proposals,'held674':excluded or {}}
    h=digest(summary)
    cur.execute('SELECT id FROM finance_mapping667.receipt WHERE company_id=%s AND policy_version=%s AND input_hash=%s',(company,EVIDENCE_POLICY,h))
    existing=cur.fetchone()
    if existing:return existing[0]
    ends=[d['end'] for d in proposals]
    cur.execute('INSERT INTO finance_mapping667.receipt(company_id,policy_version,start_date,end_date,summary,input_hash) VALUES(%s,%s,%s,%s,%s::jsonb,%s) RETURNING id',
                (company,EVIDENCE_POLICY,min(ends) if ends else date.today(),max(ends) if ends else date.today(),canonical(summary),h))
    return cur.fetchone()[0]

def capture_collected_evidence(cur,target,records):
    if not enabled() or not records.get('raw_payloads674'):return
    company=int(target['company_id'])
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended('finance_mapping667:'||%s::text,0))",(str(company),))
    try:rows=read_rows(cur,company,date(1900,1,1),date.today())
    except ValueError as exc:
        if str(exc) not in {'COMPANY_FACT_BUDGET_EXCEEDED','MAPPING_COMPANY_IDENTITY_UNVERIFIED'}:raise
        store_evidence(cur,company,[],{str(exc):1});return
    groups=defaultdict(list)
    cur.execute('SELECT metric_id,valid_from,valid_to FROM public.financial_metric_mapping WHERE company_id=%s OR company_id IS NULL',(company,))
    mappings=cur.fetchall()
    cur.execute('SELECT metric_id,valid_from FROM finance_mapping667.approved WHERE company_id=%s AND active AND valid_from=valid_to AND policy_version=ANY(%s)',(company,sorted(HISTORY_POLICIES)))
    refreshable={(m[0],str(m[1])) for m in cur.fetchall()}
    for r in rows:
        end=str(r['period_end'])
        if (r['metric_id'],end) not in refreshable and any(m[0]==r['metric_id'] and (m[1] is None or str(m[1])<=end) and (m[2] is None or str(m[2])>=end) for m in mappings):continue
        groups[(r['metric_id'],end)].append(r)
    decisions=[];held=Counter();frames=None
    for source,payload in records['raw_payloads674']:
        if content_hash(payload)!=source['content_hash']:raise ValueError('RAW_CONTENT_HASH_CHANGED')
        index=raw_index(payload,target['cik'])
        descriptor={'canonicalJsonSha256':source['content_hash'],'objectUri':source['object_uri'],
                    'officialSource':'https://data.sec.gov/api/xbrl/companyfacts/CIK'+str(int(target['cik'])).zfill(10)+'.json'}
        if any(obs['form'] in FOREIGN_ANNUAL_FORMS for obs in index.values()):
            # Reuse only already reviewed ORIGINAL documents for the same
            # accession. A new filing needs its own accounting-basis review.
            cur.execute("SELECT proof FROM finance_mapping667.approved WHERE company_id=%s AND active AND policy_version=%s AND proof->>'annualFormContract'='SEC_FOREIGN_ANNUAL_US_GAAP675_V1'",(company,POLICY))
            reviewed={}
            for (proof,) in cur.fetchall():
                for accession,review in proof.get('rawSource',{}).get('foreignAnnualReview',{}).items():
                    if accession in reviewed and reviewed[accession]!=review:raise ValueError('FOREIGN_ANNUAL_DOCUMENT_REVIEW_CONFLICT')
                    reviewed[accession]=review
            descriptor['foreignAnnualReview']=reviewed
        for facts in groups.values():
            try:decisions.append(unchanged_approval(facts,index,descriptor,foreign_annual=True))
            except ValueError as e:
                if str(e)=='SUPPORTED_SUCCESSOR_TAG_NOT_FOUND' and os.getenv('FINANCIAL_LEGACY_REVENUE693_ENABLED','false').lower()=='true':
                    try:decisions.append(legacy_revenue_approval(facts,index,descriptor,foreign_annual=True))
                    except ValueError as legacy_error:held[str(legacy_error)]+=1
                elif str(e)=='RAW_FACT_NOT_FOUND' and os.getenv('FINANCIAL_FRAME_METADATA_ENABLED','false').lower()=='true':
                    from pipeline.observation_frame686 import frame_verified_approval
                    if frames is None:frames=retained_frame_map(cur,company,rows)
                    try:decisions.append(frame_verified_approval(facts,payload,descriptor,frames,foreign_annual=True))
                    except ValueError as frame_error:held[str(frame_error)]+=1
                else:held[str(e)]+=1
    by_scope={}
    for d in decisions:
        key=(d['metric_id'],d['end'])
        if key in by_scope and by_scope[key]['source_tag']!=d['source_tag']:raise ValueError('RAW_SNAPSHOT_CONFLICT')
        by_scope[key]=d
    store_evidence(cur,company,list(by_scope.values()),dict(held))

def merge_verified_history(cur,company,start,end,annual_only,decisions,*,repair_only=False):
    if not enabled():
        if repair_only:raise ValueError('RAW_HISTORY_FEATURE_DISABLED')
        return decisions
    cur.execute('SELECT summary FROM finance_mapping667.receipt WHERE company_id=%s AND policy_version=%s ORDER BY id DESC LIMIT 1',(company,EVIDENCE_POLICY))
    record=cur.fetchone();prepared=record[0].get('proposals674',[]) if record else []
    cur.execute('SELECT metric_id,valid_from,source_tag,active,policy_version,proof,proof_hash FROM finance_mapping667.approved WHERE company_id=%s AND valid_from=valid_to AND valid_from BETWEEN %s AND %s',(company,start,end))
    existing={(r[0],str(r[1])):r for r in cur.fetchall()}
    # Existing verified approvals survive ordinary primary-only mapper replay.
    for (mid,day),r in existing.items():
        if r[3] and r[4] in HISTORY_POLICIES:
            prepared.append({'company_id':company,'metric_id':mid,'metric_code':r[5]['metricCode'],'end':day,
                             'source_tag':r[2],'proof':r[5],'proof_hash':r[6],'policy_version':r[4],'outcome':'EXISTING','reason':None})
    if not prepared:return [] if repair_only else decisions
    rows=read_rows(cur,company,start,end);groups=defaultdict(list)
    cur.execute('SELECT metric_id,company_id,taxonomy,source_tag,valid_from,valid_to FROM public.financial_metric_mapping WHERE company_id=%s OR company_id IS NULL',(company,))
    all_mappings=cur.fetchall()
    for r in rows:groups[(r['metric_id'],str(r['period_end']))].append(r)
    overrides={}
    for d in prepared:
        validate_proposal(d)
        if d['company_id']!=company:raise ValueError('RAW_EVIDENCE_COMPANY_MISMATCH')
        key=(d['metric_id'],d['end']);scope=groups.get(key,[])
        if key in overrides:continue # latest fresh raw proposal takes precedence over the old proof
        if not str(start)<=d['end']<=str(end) or not scope:continue
        if digest(d['proof'])!=d['proof_hash'] or d['proof']['policy']!=d['policy_version']:raise ValueError('RAW_EVIDENCE_CORRUPT')
        prior=existing.get(key)
        if prior and (not prior[3] or prior[4]!=d['policy_version']):continue # preserve reviewed/legacy history, never reactivate or replace a different proof contract
        matches=[m for m in all_mappings if m[0]==d['metric_id'] and (m[4] is None or str(m[4])<=d['end']) and (m[5] is None or str(m[5])>=d['end'])]
        if matches and not (prior and prior[3] and prior[4]==d['policy_version'] and all(m[1]==company and m[2]=='us-gaap' and m[3]==prior[2] for m in matches)):continue
        if annual_only:
            # A quarter-only instant must not enter an annual Go run. A fiscal
            # year-end can also contain a verified Q4 start; date-scoped tags
            # cover both just as in 667, so require the ORIGINAL annual report
            # rather than rejecting every year-end containing a Q4 fact.
            forms={'10-K'}
            if d['proof'].get('annualFormContract')=='SEC_FOREIGN_ANNUAL_US_GAAP675_V1':forms|={'20-F','40-F'}
            annual=[r for r in scope if r['form_type'].removesuffix('/A') in forms and str(r['report_date'])==d['end']
                    and ((r['metric_code'] in {'ASSETS','LIABILITIES'} and r['period_start'] is None)
                         or (r['period_start'] is not None and 335<=(date.fromisoformat(str(r['period_end']))-date.fromisoformat(str(r['period_start']))).days<=395))]
            if not annual:continue
        current_hash=scope_hash(scope)
        if current_hash!=d['proof']['sourceScopeHash']:
            if prior and prior[3] and prior[4]==d['policy_version']:
                proof={**d['proof'],'currentScopeHash':current_hash,'reason':'RAW_SOURCE_CHANGED_REVIEW_REQUIRED'}
                overrides[key]={**d,'outcome':'HELD','reason':'RAW_SOURCE_CHANGED_REVIEW_REQUIRED','proof':proof,'proof_hash':digest(proof)}
            continue # fresh raw evidence required after any source change
        if prior:d=dict(d,outcome='EXISTING')
        overrides[key]=d
    return ([] if repair_only else [d for d in decisions if (d['metric_id'],d['end']) not in overrides])+list(overrides.values())
