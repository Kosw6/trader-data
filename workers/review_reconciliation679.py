"""Build review-only candidates/cards from a frozen inventory; never writes DB.

Network is confined to exact candidate SEC URLs and bounded serial GETs. A new
accounting reconciliation still needs human registration in a reviewed bundle.
"""
import argparse,gzip,hashlib,json,re,time
from pathlib import Path
from pipeline.reconciliation_review679 import candidate,review_cards

def run(inventory,output,companies,start,end,limit,download=False):
    if not 1<=limit<=50:raise ValueError('REVIEW_COMPANY_BUDGET')
    output=Path(output);output.mkdir(parents=True,exist_ok=False);(output/'documents').mkdir()
    candidates=[];selected=set();seen=set()
    with gzip.open(inventory,'rt',encoding='utf8') as source:
        for line in source:
            d=json.loads(line)
            if d.get('kind') not in (None,'candidate'):continue
            d=d.get('data',d)
            if companies and d['companyId'] not in companies or not start<=d['end']<=end:continue
            item=candidate(d)
            if not item:continue
            key=(item['companyId'],item['end'])
            if key in seen:raise ValueError('REVIEW_DUPLICATE_SCOPE')
            if item['companyId'] not in selected and len(selected)>=limit:continue
            selected.add(item['companyId']);seen.add(key);candidates.append(item)
    (output/'candidates.json').write_text(json.dumps(candidates,indent=2),encoding='utf8')
    session=None;documents=[];cards=[];last=0;total=0
    try:
        if download:
            import requests
            session=requests.Session();session.trust_env=False;session.headers['User-Agent']='Revest Lab financial-data validation (kosw006@gmail.com)'
        urls={}
        for item in candidates:
            for url in item['urls']:
                cik=str(int(item['cik']))
                if not re.fullmatch(r'https://www\.sec\.gov/Archives/edgar/data/'+cik+r'/\d{18}/[A-Za-z0-9_.-]+',url):raise ValueError('REVIEW_SOURCE_URL_INVALID')
                urls.setdefault(url,[]).append(item)
        if len(urls)>100:raise ValueError('REVIEW_DOCUMENT_COUNT_BUDGET')
        if session:
            for url,items in urls.items():
                time.sleep(max(0,1-(time.monotonic()-last)));last=time.monotonic()
                with session.get(url,timeout=(5,40),stream=True,allow_redirects=False) as response:
                    if response.status_code!=200:raise ValueError('SEC_REVIEW_FETCH_FAILED_'+str(response.status_code))
                    chunks=[];size=0
                    for block in response.iter_content(1024*1024):
                        size+=len(block)
                        if size>32*1024**2:raise ValueError('REVIEW_DOCUMENT_BYTES_BUDGET')
                        chunks.append(block)
                    body=b''.join(chunks)
                total+=len(body)
                if total>256*1024**2:raise ValueError('REVIEW_TOTAL_BYTES_BUDGET')
                sha=hashlib.sha256(body).hexdigest()
                with gzip.open(output/'documents'/(sha+'.html.gz'),'xb') as f:f.write(body)
                documents.append({'url':url,'sha256':sha,'bytes':len(body)})
                for item in items:
                    cards.append({'companyId':item['companyId'],'end':item['end'],'url':url,'documentSha256':sha,'cards':review_cards(body,item),'requiresHumanReview':True})
                (output/'documents-progress.json').write_text(json.dumps({'documents':documents,'bytes':total}),encoding='utf8')
    finally:
        if session:session.close()
    result={'candidates':len(candidates),'companies':len(selected),'documents':documents,'cards':cards,'databaseWrites':0,'approvalsGenerated':0,'requiresHumanReview':True}
    (output/'report.json').write_text(json.dumps(result,indent=2),encoding='utf8');return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--inventory',required=True);p.add_argument('--output',required=True);p.add_argument('--company-id',type=int,action='append');p.add_argument('--start',default='1900-01-01');p.add_argument('--end',required=True);p.add_argument('--limit',type=int,default=10);p.add_argument('--download',action='store_true');a=p.parse_args()
    result=run(a.inventory,a.output,set(a.company_id or []),a.start,a.end,a.limit,a.download);print(json.dumps({k:result[k] for k in ['candidates','companies','databaseWrites','approvalsGenerated']}))
