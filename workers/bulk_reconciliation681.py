"""Bounded bulk evidence screening. NEVER generates approvals or writes a DB.

Each independent output preserves the input hash, document hashes, screening
cards and unresolved causes. Accounting hints are not accounting conclusions.
"""
import argparse,gzip,hashlib,html,json,re,time
from datetime import datetime,timezone
from collections import Counter
from decimal import Decimal,ROUND_HALF_UP
from pathlib import Path
from pipeline.reconciliation_review679 import review_cards

MAX_DOCUMENT=32*1024**2
def valid_url(url,cik):
    return bool(re.fullmatch(r'https://www\.sec\.gov/Archives/edgar/data/'+str(int(cik))+r'/\d{18}/[A-Za-z0-9_.-]+',url))

def load_plan(path):
    data=Path(path).read_bytes()
    if len(data)>2*1024**2:raise ValueError('PLAN_BYTES_BUDGET')
    items=json.loads(data)['candidates'];seen=set()
    if not 1<=len(items)<=500:raise ValueError('SCOPE_BUDGET')
    for item in items:
        key=(item['companyId'],item['end'])
        if key in seen:raise ValueError('DUPLICATE_SCOPE')
        seen.add(key)
        if not item['urls'] or any(not valid_url(u,item['cik']) for u in item['urls']):raise ValueError('SOURCE_IDENTITY_INVALID')
        if any(not Decimal(str(item[k])).is_finite() for k in ['original','chosen','adjustment']):raise ValueError('NONFINITE_AMOUNT')
        if Decimal(item['original'])+Decimal(item['adjustment'])!=Decimal(item['chosen']):raise ValueError('PLAN_EQUATION_INVALID')
    return items,hashlib.sha256(data).hexdigest()

def read_document(path,sha):
    if not re.fullmatch('[a-f0-9]{64}',sha) or path.is_symlink():raise ValueError('DOCUMENT_PATH_INVALID')
    with gzip.open(path,'rb') as f:body=f.read(MAX_DOCUMENT+1)
    if len(body)>MAX_DOCUMENT or hashlib.sha256(body).hexdigest()!=sha:raise ValueError('DOCUMENT_HASH_OR_BYTES_INVALID')
    return body

def cache_catalog(roots):
    catalog={}
    for root in roots:
        root=Path(root)
        for name in ['report.json','manifest.json']:
            file=root/name
            if not file.exists():continue
            data=json.loads(file.read_text(encoding='utf8'))
            docs=data.get('documents',[]) if name=='report.json' else [d for e in data['entries'] for d in e['documents']]
            for d in docs:
                prior=catalog.get(d['url'])
                if prior and prior[0]!=d['sha256']:raise ValueError('CACHE_DOCUMENT_CONFLICT')
                catalog[d['url']]=(d['sha256'],root/'documents'/(d['sha256']+'.html.gz'))
    return catalog

def go_batches(items,plan_sha):
    """Existing Go reviewed mode: explicit <=50 companies; never allCompanies.

    These are operator execution plans, not permission to enqueue or approve.
    A stable request key makes a later explicit submission idempotent.
    """
    companies=sorted({i['companyId'] for i in items});result=[]
    for index in range(0,len(companies),50):
        ids=companies[index:index+50];scopes=[i for i in items if i['companyId'] in ids]
        result.append({'requestKey':'bulk681:'+plan_sha+':'+str(index//50+1),
          'mode':'reviewedReconciliation','companyIds':ids,'allCompanies':False,
          'annualOnly':True,'startDate':min(i['end'] for i in scopes),'endDate':max(i['end'] for i in scopes)})
    return result

def screen(item,documents):
    cards=[];hints=set();precision=[]
    for d,body in documents:
        plain=html.unescape(re.sub('<[^>]+>',' ',body.decode('utf8',errors='replace')))
        plain=re.sub(r'\s+',' ',plain).casefold()
        for label,pattern in [('REVENUE_STANDARD',r'topic\s*606|asc\s*606'),('ERROR_CORRECTION',r'correction of.*?misstatement|restatement of.*?error'),('DISCONTINUED_OPERATIONS',r'discontinued operations'),('PRESENTATION_RECLASSIFICATION',r'reclassified.*?presentation')]:
            if re.search(pattern,plain):hints.add(label)
        for c in review_cards(body,item):cards.append({'document':d,**c})
        # A numerical rounding relationship alone is deliberately NOT approval.
        for scale in [1000,1000000]:
            a=Decimal(item['original']);b=Decimal(item['chosen'])
            if a!=b and (a/scale).quantize(Decimal(1),rounding=ROUND_HALF_UP)*scale==b:
                precision.append({'unitScale':scale,'sourceUrl':d['url'],'requiresUnitAndPeriodReview':True})
    from pipeline.revenue_scope_review695 import classify
    coverage=classify(item,documents)
    status='EXACT_EQUATION_REQUIRES_ACCOUNTING_REVIEW' if cards else 'PRECISION_COMPATIBLE_REQUIRES_DOCUMENT_REVIEW' if precision else 'NO_EXACT_RECONCILIATION_FOUND'
    return {'companyId':item['companyId'],'start':item['start'],'end':item['end'],'original':item['original'],'chosen':item['chosen'],'adjustment':item['adjustment'],'status':status,'accountingLanguageHints':sorted(hints),'hintsAreNotCauseConclusions':True,'equationCards':cards,'precisionCandidates':precision,'scopeCoverageReview':coverage,'documents':[d for d,_ in documents],'requiresHumanReview':True,'approvalGenerated':False}

def run(plan,output,cache_roots,download=False,max_bytes=2*1024**3,*,deadline_utc=None,max_new_documents=800,max_new_bytes=2*1024**3):
    items,plan_sha=load_plan(plan);out=Path(output);out.mkdir(parents=True,exist_ok=False);(out/'documents').mkdir()
    cache=cache_catalog(cache_roots);urls={}
    for item in items:
        for url in item['urls']:urls.setdefault(url,item['cik'])
    if len(urls)>800:raise ValueError('DOCUMENT_COUNT_BUDGET')
    def save(name,value):(out/name).write_text(json.dumps(value,indent=2),encoding='utf8')
    save('plan.json',{'inputSha256':plan_sha,'scopes':len(items),'companies':len({i['companyId'] for i in items}),'documents':len(urls),'maxBytes':max_bytes,'approvalGenerated':False})
    save('go-batches.json',{'batches':go_batches(items,plan_sha),'submitted':False,'newSchedulesCreated':0,'requiresRegisteredReviewedBundle':True})
    deadline=datetime.fromisoformat(deadline_utc) if deadline_utc else None
    if deadline is not None and deadline.tzinfo is None:raise ValueError('DEADLINE_TIMEZONE_REQUIRED')
    if not 1<=max_new_documents<=800 or not 1<=max_new_bytes<=2*1024**3:raise ValueError('NEW_FETCH_BUDGET_INVALID')
    def expired():return deadline is not None and datetime.now(timezone.utc)>=deadline
    session=None;docs={};failed={};total=0;last=0;stop=None;new_count=0;new_bytes=0
    if download:
        import requests
        session=requests.Session();session.trust_env=False;session.headers['User-Agent']='Revest Lab financial-data validation (kosw006@gmail.com)'
    try:
        for url in urls:
            if expired():raise ValueError('DEADLINE_REACHED')
            origin='CACHE'
            if url in cache:
                sha,path=cache[url];body=read_document(path,sha)
            elif session:
                if new_count>=max_new_documents:raise ValueError('NEW_DOCUMENT_COUNT_BUDGET')
                origin='SEC_GET';time.sleep(max(0,1-(time.monotonic()-last)));last=time.monotonic()
                with session.get(url,timeout=(5,40),stream=True,allow_redirects=False) as response:
                    if response.status_code==404:
                        failed[url]='SOURCE_NOT_FOUND_404';save('document-failures.json',failed);continue
                    if response.status_code!=200:raise ValueError('SEC_FETCH_STOP_'+str(response.status_code))
                    parts=[];size=0
                    for block in response.iter_content(1024*1024):
                        size+=len(block)
                        if size>MAX_DOCUMENT:raise ValueError('DOCUMENT_BYTES_BUDGET')
                        parts.append(block)
                    body=b''.join(parts);sha=hashlib.sha256(body).hexdigest()
                new_count+=1;new_bytes+=len(body)
                if new_bytes>max_new_bytes:raise ValueError('NEW_DOCUMENT_BYTES_BUDGET')
            else:failed[url]='CACHE_MISS_NO_FETCH';continue
            total+=len(body)
            if total>max_bytes:raise ValueError('TOTAL_BYTES_BUDGET')
            file=out/'documents'/(sha+'.html.gz')
            if not file.exists():
                with gzip.open(file,'xb') as f:f.write(body)
            docs[url]={'url':url,'sha256':sha,'bytes':len(body),'origin':origin}
            save('documents-progress.json',{'documents':list(docs.values()),'bytes':total})
    except Exception as e:
        stop=str(e) if isinstance(e,ValueError) else type(e).__name__
    finally:
        if session:session.close()
    save('report.json',{'documents':list(docs.values()),'failedDocuments':failed,'stopReason':stop})
    results=[]
    with (out/'screened-scopes.jsonl').open('x',encoding='utf8') as stream:
        for item in items:
            missing=[u for u in item['urls'] if u not in docs]
            if expired():
                stop='DEADLINE_REACHED';r={'companyId':item['companyId'],'end':item['end'],'status':'NOT_SCREENED_DEADLINE','requiresHumanReview':True,'approvalGenerated':False}
            elif missing:r={'companyId':item['companyId'],'end':item['end'],'status':'DOCUMENTS_INCOMPLETE','missingUrls':missing,'requiresHumanReview':True,'approvalGenerated':False}
            else:r=screen(item,[(docs[u],read_document(out/'documents'/(docs[u]['sha256']+'.html.gz'),docs[u]['sha256'])) for u in item['urls']])
            stream.write(json.dumps(r)+'\n');stream.flush();results.append(r)
    summary={'status':'SCREENING_STOPPED' if stop else 'SCREENING_COMPLETE_REVIEW_REQUIRED','scopes':len(items),'companies':len({i['companyId'] for i in items}),'documents':len(docs),'downloaded':sum(d['origin']=='SEC_GET' for d in docs.values()),'cached':sum(d['origin']=='CACHE' for d in docs.values()),'rawBytes':total,'newRawBytes':new_bytes,'deadlineUTC':deadline_utc,'classification':dict(Counter(r['status'] for r in results)),'stopReason':stop,'databaseWrites':0,'approvalsGenerated':0,'inputSha256':plan_sha}
    save('summary.json',summary);return summary

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--plan',required=True);p.add_argument('--output',required=True);p.add_argument('--cache-root',action='append',default=[]);p.add_argument('--download',action='store_true');p.add_argument('--deadline-utc');p.add_argument('--max-new-documents',type=int,default=800);p.add_argument('--max-new-bytes',type=int,default=2*1024**3);a=p.parse_args()
    print(json.dumps(run(a.plan,a.output,a.cache_root,a.download,deadline_utc=a.deadline_utc,max_new_documents=a.max_new_documents,max_new_bytes=a.max_new_bytes)))
