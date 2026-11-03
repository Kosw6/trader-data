"""Bounded DOC queries and three 2016 GKG snapshots; no keys, AWS or BigQuery."""
import json
import sys
from datetime import datetime,timedelta,timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'trader-data'))
from collectors.gdelt_client import GdeltClient,GdeltError,DOC,DATA,parse_doc,parse_gkg


def save(path,data):
    with path.open('x',encoding='utf-8') as f:json.dump(data,f,ensure_ascii=False,indent=2)


def main():
    now=datetime.now(timezone.utc)
    out=ROOT/'output'/('news-gdelt-eval-'+now.strftime('%Y%m%dT%H%M%SZ'))
    out.mkdir(parents=True,exist_ok=False)
    stamps=['20161214200000','20161230141500','20160624120000']
    cases=[('recent_NVDA','Nvidia sourcelang:english'),
           ('recent_AAPL','Apple (iPhone OR earnings OR stock) sourcelang:english'),
           ('recent_TSLA','Tesla sourcelang:english'),
           ('macro_rates','"Federal Reserve" sourcelang:english'),
           ('macro_geopolitics','(sanctions OR tariffs) sourcelang:english')]
    save(out/'plan.json',dict(maxHttpRequests=12,minimumSpacingSeconds=5.1,
        maximumResponseBytes=32*1024**2,articlesPerDocQuery=5,docQueries=cases,
        historyDocDiagnostic=True,gkg2016Snapshots=stamps,apiKey=False,aws=False,dbWrites=False,
        fullArticleCrawling=False,bigQuery=False))
    c=GdeltClient();results=[];articles=[];archives=[];doc_failures=0
    queries=[(name,dict(query=query,startdatetime=(now-timedelta(days=7)).strftime('%Y%m%d%H%M%S'),
                   enddatetime=now.strftime('%Y%m%d%H%M%S'))) for name,query in cases]
    queries.append(('doc_2016_diagnostic',dict(query='Nvidia',startdatetime='20161230000000',enddatetime='20161230235959')))
    for name,params in queries:
        if doc_failures>=2:
            results.append(dict(case=name,status='SKIPPED_DOC_HOST_UNAVAILABLE'));continue
        params.update(mode='artlist',format='json',maxrecords=5,sort='datedesc')
        save(out/(name+'-intent.json'),dict(url=DOC,parameters=params))
        try:
            raw,receipt=c.get(DOC,params)
            save(out/(name+'-receipt.json'),receipt)
            parsed=parse_doc(raw)
            for a in parsed:a['case']=name
            articles+=parsed
            save(out/(name+'.json'),parsed)
            invalid=[a['url'] for a in parsed if not params['startdatetime'][:8]<=a['seenAt'][:10].replace('-','')<=params['enddatetime'][:8]]
            results.append(dict(case=name,status='SAMPLE_RETURNED' if parsed else 'EMPTY_SAMPLE_NOT_COVERAGE_PROOF',returned=len(parsed),dateViolations=invalid))
            doc_failures=0
        except GdeltError as e:
            error=dict(case=name,status='UNAVAILABLE_OR_REJECTED',code=e.code,receipt=e.receipt)
            save(out/(name+'-error.json'),error);results.append(error);doc_failures+=1
        print(json.dumps(results[-1]),flush=True)
    for stamp in stamps:
        name='gkg_'+stamp;url=DATA+stamp+'.gkg.csv.zip'
        save(out/(name+'-intent.json'),dict(url=url))
        try:
            raw,receipt=c.get(url)
            with (out/(name+'.zip')).open('xb') as f:f.write(raw)
            save(out/(name+'-receipt.json'),receipt)
            parsed=parse_gkg(raw,stamp)
            save(out/(name+'.json'),parsed);archives.append(parsed)
            results.append(dict(case=name,status='ARCHIVE_SNAPSHOT_PARSED',rows=parsed['rows'],
                                invalidRows=parsed['invalidRows'],candidateCounts=parsed['candidateCounts']))
        except GdeltError as e:
            error=dict(case=name,status='UNAVAILABLE_OR_REJECTED',code=e.code,receipt=e.receipt)
            save(out/(name+'-error.json'),error);results.append(error)
        print(json.dumps(results[-1]),flush=True)
    c.close()
    save(out/'doc-articles.json',articles)
    save(out/'case-results.json',results)
    report=dict(status='EVALUATED_WITH_LIMITATIONS',httpRequests=c.attempts,responseBytes=c.bytes,
                docArticleAppearances=len(articles),docUniqueUrls=len({a['url'] for a in articles}),
                parsed2016Snapshots=len(archives),historicalMetadataRows=sum(x['rows'] for x in archives),
                historicalSamples=sum(len(x['samples']) for x in archives),
                docFailures=[r['case'] for r in results if r.get('code')],
                observationTimesAreNotVerifiedPublicationTimes=True,bodyProvided=False,
                allYearCoverageVerified=False,apiKeyUsed=False,paidServicesUsed=False,awsChanges=0,dbWrites=0)
    save(out/'summary.json',report)
    print(json.dumps({'directory':str(out),**report}),flush=True)


if __name__=='__main__':main()
