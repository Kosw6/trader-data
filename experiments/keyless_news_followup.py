"""Finite follow-up, preserving failed GDELT receipts; no cloud or paid services."""
import hashlib
import json
import sys
import time
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'trader-data'))
from collectors.gdelt_client import GdeltClient,GdeltError,DATA,parse_gkg
from collectors.google_news_rss import URL,parse_feed


def save(path,value):
    with path.open('x',encoding='utf-8') as f:json.dump(value,f,ensure_ascii=False,indent=2)


def main():
    parent=ROOT/'output/news-gdelt-eval-20261101T133522Z'
    old=json.loads((parent/'summary.json').read_text(encoding='utf-8'))
    out=parent/'keyless-followup';out.mkdir(exist_ok=False)
    save(out/'plan.json',dict(previousGdeltUsage=old,additionalHeaderRequests=3,
         combinedResponseByteCap=64*1024**2,totalScriptHttpCap=20,
         archiveSelections=['20161214200000','20161230141500'],
         selectionReason='HEAD confirmed 16,143,631 and 10,629,865 bytes; original 10MiB per-file cap was too low',
         rssQueries=5,rssFormatIsNotGuaranteedHistoricalApi=True,apiKey=False,aws=False,dbWrites=False))
    c=GdeltClient(max_bytes=64*1024**2,archive_limit=20*1024**2)
    c.bytes=old['responseBytes'];c.attempts=old['httpRequests']+3
    archives=[];rss=[];errors=[]
    for stamp in ['20161214200000','20161230141500']:
        try:
            raw,receipt=c.get(DATA+stamp+'.gkg.csv.zip')
            with (out/('gkg_'+stamp+'.zip')).open('xb') as f:f.write(raw)
            save(out/('gkg_'+stamp+'-receipt.json'),receipt)
            parsed=parse_gkg(raw,stamp);save(out/('gkg_'+stamp+'.json'),parsed)
            archives.append(parsed)
            print(json.dumps({'archive':stamp,'rows':parsed['rows'],'invalidRows':parsed['invalidRows'],'candidateCounts':parsed['candidateCounts']}),flush=True)
        except GdeltError as e:
            row=dict(case='gkg_'+stamp,code=e.code,receipt=e.receipt)
            save(out/('gkg_'+stamp+'-error.json'),row);errors.append(row)
    c.close()
    s=requests.Session();s.trust_env=False;s.mount('https://',HTTPAdapter(max_retries=0))
    s.headers.update({'User-Agent':'RevestLab-News-RSS-Evaluation/1.0'})
    rss_attempts=0;rss_bytes=0
    for name,term in [('NVDA','NVIDIA'),('AAPL','Apple iPhone'),('TSLA','Tesla Motors'),
                      ('macro_rates','"Federal Reserve"'),('macro_geopolitics','sanctions')]:
        query=term+' after:2015-12-31 before:2017-01-01'
        params=dict(q=query,hl='en-US',gl='US',ceid='US:en')
        save(out/('rss_'+name+'-intent.json'),dict(url=URL,parameters=params))
        if rss_attempts:time.sleep(5.1)
        rss_attempts+=1
        try:
            with s.get(URL,params=params,stream=True,timeout=(5,25),allow_redirects=False) as r:
                body=bytearray()
                for chunk in r.iter_content(65536):
                    rss_bytes+=len(chunk);body.extend(chunk)
                    if len(body)>1024**2 or c.bytes+rss_bytes>64*1024**2:raise ValueError('RSS_BYTE_CAP')
                receipt=dict(status=r.status_code,bytes=len(body),sha256=hashlib.sha256(body).hexdigest(),parameters=params)
                save(out/('rss_'+name+'-receipt.json'),receipt)
                if r.status_code!=200:
                    raise ValueError('RSS_HTTP_'+str(r.status_code))
                parsed=parse_feed(body)
                save(out/('rss_'+name+'.json'),parsed);rss.append(dict(case=name,**parsed))
                print(json.dumps({'rss':name,'returned':parsed['returnedItems'],'matching2016':parsed['matchingYearItems'],'outOfYear':parsed['outOfYearItems']}),flush=True)
        except (requests.RequestException,ValueError) as e:
            code=type(e).__name__ if isinstance(e,requests.RequestException) else str(e)
            row=dict(case='rss_'+name,code=code);save(out/('rss_'+name+'-error.json'),row);errors.append(row)
            if code=='RSS_HTTP_429':break
    s.close()
    report=dict(status='KEYLESS_SAMPLES_EVALUATED_WITH_LIMITATIONS',
         scriptHttpRequests=c.attempts+rss_attempts,responseBytes=c.bytes+rss_bytes,
         parsed2016GkgSnapshots=len(archives),gkgRows=sum(x['rows'] for x in archives),
         gkgCandidateCounts={k:sum(x['candidateCounts'].get(k,0) for x in archives)
                            for k in ['NVDA','AAPL','TSLA','MACRO_RATES_CANDIDATE','MACRO_GEOPOLITICS_CANDIDATE']},
         rssCases=[{k:x[k] for k in ['case','returnedItems','matchingYearItems','outOfYearItems','uniqueLinks']} for x in rss],
         errors=errors,fullTextProvided=False,verifiedTickerIdentity=False,
         wholeYearCoverageVerified=False,paidServicesUsed=False,awsActions=0,dbWrites=0)
    save(out/'completion.json',report)
    print(json.dumps(report),flush=True)


if __name__=='__main__':main()
