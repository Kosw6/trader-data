"""Use remaining five requests of the twenty-request keyless experiment."""
import hashlib
import json
import sys
import time
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter

ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'trader-data'))
from collectors.google_news_rss import URL,parse_feed
from collectors.gdelt_client import parse_gkg


def save(p,value):
    with p.open('x',encoding='utf-8') as f:json.dump(value,f,ensure_ascii=False,indent=2)


def main():
    parent=ROOT/'output/news-gdelt-eval-20261101T133522Z/keyless-followup'
    before=json.loads((parent/'completion.json').read_text(encoding='utf-8'))
    assert before['scriptHttpRequests']==15
    out=parent/'refined-rss';out.mkdir(exist_ok=False)
    queries=[('NVDA','NVIDIA (earnings OR revenue OR shares)'),
             ('AAPL','Apple (earnings OR revenue OR shares)'),
             ('TSLA','Tesla (earnings OR deliveries OR shares)'),
             ('macro_rates','"Federal Reserve" (FOMC OR "interest rates" OR hike)'),
             ('macro_geopolitics','sanctions (Iran OR Russia OR "North Korea")')]
    save(out/'plan.json',dict(previousScriptHttpRequests=15,remainingRequests=5,cumulativeHttpRequestCap=20,
         cumulativeResponseByteCap=64*1024**2,queries=queries,
         reason='Broad queries returned technical blogs, administrative pages and evergreen charts'))
    # Re-interpret date fields using the official GKG 2.1 codebook; retain old receipts.
    for stamp in ['20161214200000','20161230141500']:
        parsed=parse_gkg((parent/('gkg_'+stamp+'.zip')).read_bytes(),stamp)
        save(out/('gkg_'+stamp+'-reviewed.json'),parsed)
    s=requests.Session();s.trust_env=False;s.mount('https://',HTTPAdapter(max_retries=0))
    s.headers.update({'User-Agent':'RevestLab-News-RSS-Evaluation/1.0'})
    used=before['responseBytes'];attempts=0;results=[]
    for name,term in queries:
        params=dict(q=term+' after:2015-12-31 before:2017-01-01',hl='en-US',gl='US',ceid='US:en')
        save(out/(name+'-intent.json'),dict(url=URL,parameters=params))
        if attempts:time.sleep(5.1)
        attempts+=1
        try:
            with s.get(URL,params=params,stream=True,timeout=(5,25),allow_redirects=False) as r:
                raw=bytearray()
                for b in r.iter_content(65536):
                    used+=len(b);raw.extend(b)
                    assert len(raw)<=1024**2 and used<=64*1024**2,'COMBINED_BYTE_CAP'
                save(out/(name+'-receipt.json'),dict(status=r.status_code,parameters=params,bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest()))
                if r.status_code!=200:raise ValueError('RSS_HTTP_'+str(r.status_code))
                data=parse_feed(raw);save(out/(name+'.json'),data)
                row=dict(case=name,**{k:data[k] for k in ['returnedItems','matchingYearItems','invalidItems','outOfYearItems','uniqueLinks']})
                results.append(row);print(json.dumps(row),flush=True)
        except (requests.RequestException,ValueError) as e:
            code=type(e).__name__ if isinstance(e,requests.RequestException) else str(e)
            row=dict(case=name,code=code);save(out/(name+'-error.json'),row);results.append(row)
            if code=='RSS_HTTP_429':break
    s.close()
    save(out/'completion.json',dict(status='BOUNDED_REFINED_RSS_FINISHED',scriptHttpRequests=15+attempts,
        responseBytes=used,results=results,articleBodiesProvided=False,
        originalPublicationTimesVerified=False,wholeYearCompletenessVerified=False,
        apiKeyUsed=False,awsChanges=0,dbWrites=0))


if __name__=='__main__':main()
