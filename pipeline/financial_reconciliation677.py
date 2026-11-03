"""Replay explicitly reviewed reconciliation bundles; never infer accounting intent.

The operator supplies an immutable, read-only bundle. Network retrieval is a
separate preparation step, never performed while holding a database lock.
"""
import gzip
import hashlib
import json
import os
import re
from collections import Counter
from pathlib import Path
from pipeline.reviewed_financial_mapping672 import reviewed_approval, canonical, digest, POLICY

CONTRACT='SEC_REVIEWED_RECONCILIATION677_V1'
REASON='REVIEWED_RECONCILIATION677'
SCOPE_SQL="""SELECT f.id AS fact_id,f.company_id,f.metric_id,m.metric_code,f.taxonomy,f.source_tag,
 f.unit,f.period_start,f.period_end,f.value,f.dimensions,f.source_fact_hash,g.accession_number,
 g.filing_date,g.report_date,g.document_url AS source_url,g.form_type,c.cik,f.fiscal_year,f.fiscal_period
 FROM public.financial_fact f JOIN public.financial_metric m ON m.id=f.metric_id
 JOIN public.filing g ON g.id=f.filing_id AND g.company_id=f.company_id
 JOIN public.company c ON c.id=f.company_id
 WHERE f.company_id=%s AND m.metric_code='REVENUE' AND f.period_start=%s AND f.period_end=%s
 AND f.taxonomy='us-gaap' AND f.unit='USD' AND (f.dimensions IS NULL OR f.dimensions='{}'::jsonb)
 ORDER BY f.id LIMIT 10001"""

def load_bundle(path):
    path=Path(path)
    if path.stat().st_size>2*1024**2:raise ValueError('RECONCILIATION_MANIFEST_BUDGET')
    bundle=json.loads(path.read_text(encoding='utf8'))
    if bundle.get('contract')!=CONTRACT or not isinstance(bundle.get('entries'),list) or len(bundle['entries'])>1000:
        raise ValueError('RECONCILIATION_MANIFEST_INVALID')
    keys=set()
    for entry in bundle['entries']:
        review=entry['review'];key=(review['companyId'],review['metricId'],review['end'])
        if key in keys:raise ValueError('RECONCILIATION_DUPLICATE_SCOPE')
        keys.add(key)
        from pipeline.known_corrections712 import SCOPES
        if (review['companyId'],review.get('metric'),review.get('start'),review['end']) in SCOPES:
            raise ValueError('KNOWN_LATER_CORRECTION_REGISTERED_SCOPE_BLOCKED')
    from pipeline.later_correction708 import validate_watchlist
    blocked=validate_watchlist(bundle,path.parent)
    if keys & set(blocked):raise ValueError('LATE_CORRECTION_REGISTERED_SCOPE_BLOCKED')
    return bundle,path.parent

def verify_documents(entry,root):
    review=entry['review'];documents=entry.get('documents',[])
    if not documents or len(documents)>8:raise ValueError('RECONCILIATION_DOCUMENT_REQUIRED')
    expected=set(review['officialDocuments']);seen=set()
    for document in documents:
        sha=document['sha256'];url=document['url']
        if not re.fullmatch('[a-f0-9]{64}',sha):raise ValueError('RECONCILIATION_DOCUMENT_HASH_INVALID')
        # Only the reviewed company's SEC archive is an admissible source.
        cik=str(int(entry['cik']));prefix='https://www.sec.gov/Archives/edgar/data/'+cik+'/'
        if not url.startswith(prefix) or not re.fullmatch(r'https://www\.sec\.gov/Archives/edgar/data/\d+/\d{18}/[A-Za-z0-9_.-]+',url):
            raise ValueError('RECONCILIATION_DOCUMENT_IDENTITY_INVALID')
        file=root/'documents'/(sha+'.html.gz')
        if file.is_symlink():raise ValueError('RECONCILIATION_DOCUMENT_PATH_INVALID')
        with gzip.open(file,'rb') as stream:body=stream.read(32*1024**2+1)
        if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=sha:
            raise ValueError('RECONCILIATION_DOCUMENT_CHANGED')
        seen.add(url)
    if seen!=expected:raise ValueError('RECONCILIATION_DOCUMENT_SET_MISMATCH')

def validate_entry(scope,entry,root):
    verify_documents(entry,root)
    if not scope or any(str(int(r['cik']))!=str(int(entry['cik'])) for r in scope):
        raise ValueError('RECONCILIATION_COMPANY_MISMATCH')
    from pipeline.serving_source_review20261101 import ALPHABET,TESLA
    if entry['review'].get('basis') in {ALPHABET,TESLA}:
        from pipeline.serving_source_review20261101 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_INTERGROUP_CONTINUING_SEGMENT_TOTAL739':
        from pipeline.intergroup_continuing739 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_UNIVERSAL_CONTRACT_SUBTOTAL_TOTAL738':
        from pipeline.universal_total738 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_FONAR_EXTERNAL_TOTAL_VS_INTERSEGMENT732':
        from pipeline.fonar_external732 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_FORD_AUTOMOTIVE_FINANCIAL_TOTAL734':
        from pipeline.ford_total734 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_HEALTHSTREAM_PX_DISCONTINUED_REVENUE731':
        from pipeline.healthstream_px731 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_BIOMERICA_EXACT_PRIMARY_APPROXIMATE_GEOGRAPHIC730':
        from pipeline.biomerica_total730 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_OPTIMIZERX_REVENUE_SHARE_EXPENSE_ERROR728':
        from pipeline.optimizerx_error728 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_ALASKA_EXPLICIT_SPLIT_DILUTED_EPS727':
        from pipeline.alaska_split727 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_METHODE_OPERATING_PRIMARY_PRECISION726':
        from pipeline.methode_precision726 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_YELP_FULL_RETROSPECTIVE_PRIMARY724':
        from pipeline.yelp_restatement724 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_EXTR_SHARED_UNIT_FULL_RETROSPECTIVE725':
        from pipeline.extr_restatement725 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_SPLIT_DATE_PRIMARY_PRECISION_CHAIN713':
        from pipeline.split_precision713 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_RELATED_PARTY_PRIMARY_PRECISION714':
        from pipeline.related_precision714 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_EXPLICIT_SERVICE_REVENUE_ERROR_CORRECTION':
        from pipeline.service_error708 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis') in {'REVIEWED_PRIMARY_STATEMENT_PRECISION_CHAIN','REVIEWED_PRIMARY_PROPERTY_SERVICES_TOTAL_PRECISION'}:
        from pipeline.primary_precision707 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_RETAIL_AND_CREDIT_REVENUE_COMPONENT_COVERAGE':
        from pipeline.retail_coverage706 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_PRODUCT_COVERAGE_AND_GROSS_NET_ERROR':
        from pipeline.component_gross_error704 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_FULL_STATEMENT_DISCONTINUED_REVENUE':
        from pipeline.full_discontinued702 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_COMPONENT_COVERAGE_THEN_DISCONTINUED_REVENUE':
        from pipeline.coverage_discontinued701 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_LEGACY_FULL_STATEMENT_TOTAL':
        from pipeline.legacy_statement_total699 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis') in {'REVIEWED_NESTED_REVENUE_COMPONENT_COVERAGE','REVIEWED_ORIGINAL_ROUNDED_TO_DETAILED_REVENUE'}:
        from pipeline.revenue_bridge698 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_ORIGINAL_REVENUE_COMPONENT_COVERAGE':
        from pipeline.revenue_coverage694 import approve
        return approve(scope,entry,root)
    # This strict existing policy binds all stored rows, exact annual dates,
    # original/latest accession+fact hashes and Decimal(original+adjustment).
    if entry['review'].get('basis')=='EXPLICIT_ERROR_CORRECTION':
        from pipeline.error_correction680 import error_approval,verify_error_document
        approval=error_approval(scope,entry['review']);verify_error_document(entry,root);return approval
    if entry['review'].get('basis')=='EXPLICIT_PRESENTATION_PRECISION':
        from pipeline.precision_evidence682 import precision_approval
        return precision_approval(scope,entry,root)
    if entry['review'].get('basis') in {'EXPLICIT_PRESENTATION_PRECISION_LAYOUT','EXPLICIT_MULTICOMPONENT_ERROR_CORRECTION'}:
        from pipeline.reviewed_evidence683 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_MULTI_PERIOD_PRECISION_CONSISTENCY':
        from pipeline.precision_series684 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis') in {'REVIEWED_BOUND_REVENUE_STANDARD_TABLE','REVIEWED_AMENDED_ORIGINAL_CONFIRMATION','REVIEWED_BOUND_ERROR_CORRECTION_TABLE'}:
        from pipeline.bound_reconciliation685 import approve
        return approve(scope,entry,root)
    if entry['review'].get('basis')=='REVIEWED_DISCONTINUED_REVENUE_REPRESENTATION':
        from pipeline.discontinued_bridge687 import approve
        return approve(scope,entry,root)
    return reviewed_approval(scope,entry['review'])

def entry_scope_query(company,review):
    """Keep the original revenue query; other metrics need a named contract."""
    from pipeline.serving_source_review20261101 import ALPHABET,TESLA
    if review.get('basis') in {ALPHABET,TESLA}:
        from pipeline.serving_source_review20261101 import scope_query
        return scope_query(SCOPE_SQL,company,review)
    if review.get('basis')=='REVIEWED_OPTIMIZERX_REVENUE_SHARE_EXPENSE_ERROR728':
        from pipeline.optimizerx_error728 import metadata
        metadata(company,review)
        return SCOPE_SQL.replace("m.metric_code='REVENUE'","m.metric_code='"+review['metric']+"'"),(company,review['start'],review['end'])
    if review.get('basis')=='REVIEWED_ALASKA_EXPLICIT_SPLIT_DILUTED_EPS727':
        from pipeline.alaska_split727 import metadata
        metadata(company,review)
        return SCOPE_SQL.replace("m.metric_code='REVENUE'","m.metric_code='DILUTED_EPS'").replace("f.unit='USD'","f.unit='USD/shares'"),(company,review['start'],review['end'])
    if review.get('basis')=='REVIEWED_METHODE_OPERATING_PRIMARY_PRECISION726':
        from pipeline.methode_precision726 import metadata
        metadata(company,review)
        return SCOPE_SQL.replace("m.metric_code='REVENUE'","m.metric_code='OPERATING_INCOME'"),(company,review['start'],review['end'])
    if review.get('metric')!='REVENUE':
        raise ValueError('RECONCILIATION_NONREVENUE_CONTRACT_NOT_REGISTERED726')
    return SCOPE_SQL,(company,review['start'],review['end'])


def discover_company(cur,company,start,end,entries):
    from pipeline.reconciliation_review679 import candidate
    sql=SCOPE_SQL.replace('f.period_start=%s AND f.period_end=%s','f.period_end BETWEEN %s AND %s')
    cur.execute(sql,(company,start,end))
    rows=[dict(zip([d[0] for d in cur.description],r)) for r in cur.fetchall()]
    if len(rows)>10000:raise ValueError('RECONCILIATION_DISCOVERY_BUDGET')
    # Discovery historically accepts compact revenue-only identity summaries.
    # An absent metric retains that contract; an explicit other metric cannot
    # replace the revenue identity for the same period end.
    groups={};registered={e['review']['end']:e['review'] for e in entries if e['review'].get('metric','REVENUE')=='REVENUE'}
    for row in rows:groups.setdefault(str(row['period_end']),[]).append(row)
    result=[]
    for day,group in groups.items():
        prior=registered.get(day)
        if prior and digest([r for r in group if str(r['period_start'])==prior['start']])==prior['sourceScopeHash']:continue
        # Database dates are normalized for the review-only discovery helper.
        normalized=[{k:str(v) if k in {'period_start','period_end','report_date','filing_date'} and v is not None else v for k,v in r.items()} for r in group]
        item=candidate({'companyId':company,'metric':'REVENUE','end':day,'rows':normalized})
        if item:result.append(item)
    if len(result)>100:raise ValueError('RECONCILIATION_DISCOVERY_BUDGET')
    return result

def apply_reviewed_company(cur,company,start,end,*,annual_only=False):
    path=os.getenv('FINANCIAL_RECONCILIATION_MANIFEST','')
    if not path:raise ValueError('RECONCILIATION_MANIFEST_NOT_CONFIGURED')
    bundle,root=load_bundle(path)
    entries=[e for e in bundle['entries'] if e['review']['companyId']==company and str(start)<=e['review']['end']<=str(end)]
    if len(entries)>100:raise ValueError('RECONCILIATION_COMPANY_BUDGET')
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended('finance_mapping667:'||%s::text,0))",(str(company),))
    results=Counter();held=Counter();proofs=[];validated=[]
    from pipeline.later_correction708 import validate_watchlist
    blocked=[v for (cid,mid,day),v in validate_watchlist(bundle,root).items() if cid==company and str(start)<=day<=str(end)]
    for item in blocked:
        cur.execute('SELECT cik FROM public.company WHERE id=%s',(company,))
        identity=cur.fetchone()
        if not identity or int(identity[0])!=int(item['cik']):raise ValueError('LATE_CORRECTION_COMPANY_MISMATCH')
        results['HELD']+=1;held['KNOWN_LATER_DOCUMENT_CORRECTION708']+=1
    from pipeline.known_corrections712 import SCOPES,SOURCE_SHA256,verify_source,REASON as LATE_REASON
    late=[s for s in SCOPES if s[0]==company and str(start)<=s[3]<=str(end)]
    guarded_keys={(v['metricId'],v['end']) for v in blocked}
    late_evidence=[]
    if late:
        with gzip.open(root/'documents'/(SOURCE_SHA256+'.html.gz'),'rb') as f:body=f.read(32*1024**2+1)
        late_evidence=[r for r in verify_source(body) if r['companyId']==company and str(start)<=r['end']<=str(end)]
        cur.execute('SELECT cik FROM public.company WHERE id=%s',(company,));identity=cur.fetchone()
        if not identity or int(identity[0])!=1262039:raise ValueError('KNOWN_CORRECTION_COMPANY_CHANGED')
        cur.execute('SELECT metric_code,id FROM public.financial_metric WHERE metric_code=ANY(%s)',(sorted({s[1] for s in late}),));metrics=dict(cur.fetchall())
        if set(metrics)!={s[1] for s in late}:raise ValueError('KNOWN_CORRECTION_METRIC_SCHEMA_CHANGED')
        for cid,metric,begin,finish in late:
            key=(metrics[metric],finish)
            if key in guarded_keys:continue
            guarded_keys.add(key);results['HELD']+=1;held[LATE_REASON]+=1
    for entry in entries:
        review=entry['review']
        scope_sql,scope_params=entry_scope_query(company,review)
        cur.execute(scope_sql,scope_params)
        scope=[dict(zip([d[0] for d in cur.description],row)) for row in cur.fetchall()]
        if len(scope)>10000:raise ValueError('RECONCILIATION_SOURCE_BUDGET')
        # Source changes are review outcomes, not permission to rewrite facts.
        if not scope or digest(scope)!=review['sourceScopeHash']:
            results['HELD']+=1;held['RECONCILIATION_SOURCE_CHANGED']+=1;continue
        approval=validate_entry(scope,entry,root)
        policy=approval['proof']['policy']
        validated.append({'proofHash':approval['proof_hash'],'documents':entry['documents']})
        cur.execute('SELECT active,source_tag,proof_hash,policy_version FROM finance_mapping667.approved WHERE company_id=%s AND metric_id=%s AND valid_from=%s AND valid_to=%s FOR UPDATE',
                    (company,review['metricId'],review['end'],review['end']))
        existing=cur.fetchall()
        if existing:
            if len(existing)==1 and tuple(existing[0])==(True,review['chosenTag'],approval['proof_hash'],policy):results['EXISTING']+=1
            else:results['HELD']+=1;held['EXISTING_APPROVAL_PRESERVED']+=1
            continue
        cur.execute('SELECT 1 FROM public.financial_metric_mapping WHERE (company_id=%s OR company_id IS NULL) AND metric_id=%s AND (valid_from IS NULL OR valid_from<=%s) AND (valid_to IS NULL OR valid_to>=%s) LIMIT 1',
                    (company,review['metricId'],review['end'],review['end']))
        if cur.fetchone():results['HELD']+=1;held['EXISTING_MAPPING_PRESERVED']+=1;continue
        cur.execute("""INSERT INTO finance_mapping667.approved(company_id,metric_id,taxonomy,source_tag,priority,valid_from,valid_to,policy_version,proof,proof_hash,active)
         VALUES(%s,%s,'us-gaap',%s,100,%s,%s,%s,%s::jsonb,%s,TRUE)""",
         (company,review['metricId'],review['chosenTag'],review['end'],review['end'],policy,canonical(approval['proof']),approval['proof_hash']))
        results['INSERTED']+=1;proofs.append(approval['proof_hash'])
    if not entries and not guarded_keys:results['HELD']+=1;held['RECONCILIATION_REVIEW_NOT_REGISTERED']+=1
    candidates=discover_company(cur,company,start,end,entries)
    summary={'contract':CONTRACT,'bundleHash':digest(bundle),'validatedEvidence':validated,'reviewCandidates':candidates,'results':dict(results),'excluded':dict(held),'insertedProofs':proofs,'reviewedEntries':len(entries),'factWrites':0,'filingWrites':0,
             **({'knownLaterCorrectionEvidence':late_evidence,'knownLaterCorrectionEvidenceHash':digest(late_evidence)} if late_evidence else {})}
    cur.execute('INSERT INTO finance_mapping667.receipt(company_id,policy_version,start_date,end_date,summary,input_hash) VALUES(%s,%s,%s,%s,%s::jsonb,%s) RETURNING id',
                (company,CONTRACT,start,end,canonical(summary),digest(summary)))
    return {'companyId':company,'receiptId':cur.fetchone()[0],'results':dict(results),'excluded':dict(held)}
