"""Finite local-only news evaluation; no AWS, DB, model, scheduler or article fetch."""
import argparse,json,os,sys
from datetime import datetime,timedelta,timezone
from pathlib import Path
from dotenv import dotenv_values
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'trader-data'))
from collectors.marketaux_client import MarketauxClient,MarketauxError,TOKEN_ENV,assessment

def plan(now):
    until=now.replace(microsecond=0);since=until-timedelta(days=30)
    window=dict(published_after=since.strftime('%Y-%m-%dT%H:%M:%S'),published_before=until.strftime('%Y-%m-%dT%H:%M:%S'))
    queries=[]
    for symbol,name in [('NVDA','NVIDIA'),('AAPL','Apple Inc'),('TSLA','Tesla Inc')]:
        queries.append(dict(case='entity_'+symbol,endpoint='entity/search',parameters=dict(symbols=symbol,types='equity',countries='us'),expectedSymbol=symbol))
        queries.append(dict(case='ticker_'+symbol,endpoint='news/all',parameters=dict(**window,symbols=symbol,countries='us',filter_entities='true',group_similar='false',language='en',limit=3,page=1),expectedSymbol=symbol,paginate=True))
        # Keyword search otherwise defaults to relevance, which can return
        # older stories even when a recent date window is explicitly supplied.
        queries.append(dict(case='name_'+symbol,endpoint='news/all',parameters=dict(**window,search='"'+name+'"',sort='published_at',group_similar='false',language='en',limit=3,page=1),expectedSymbol=symbol))
    for year in [2016,2020,2024]:
        queries.append(dict(case='history_NVDA_'+str(year),endpoint='news/all',parameters=dict(symbols='NVDA',countries='us',filter_entities='true',group_similar='false',language='en',published_after=f'{year}-01-01T00:00:00',published_before=f'{year}-12-31T23:59:59',limit=3,page=1),expectedSymbol='NVDA',historical=True))
    for name,query in [('rates','"Federal Reserve" | "interest rates"'),('inflation','"consumer price index" | "inflation"'),('geopolitics','"tariffs" | "sanctions" | "geopolitical"')]:
        queries.append(dict(case='macro_'+name,endpoint='news/all',parameters=dict(**window,search=query,sort='published_at',group_similar='true',language='en',limit=3,page=1),topicOnly=True))
    queries.append(dict(case='macro_korean',endpoint='news/all',parameters=dict(**window,search='금리 | 관세',language='ko',group_similar='true',limit=3,page=1),topicOnly=True))
    assert len(queries)+sum(bool(q.get('paginate')) for q in queries)<=20
    return dict(provider='MARKETAUX',maxRequests=20,plannedMaximum=19,limitPerNewsRequest=3,planAssumption='FREE_COMPATIBLE_NO_UPGRADE',queries=queries)

def save(path,value):
    # Each run owns a new directory; no previous receipt is overwritten.
    with path.open('x',encoding='utf8') as f:json.dump(value,f,indent=2,ensure_ascii=False)

def run(out,token,now):
    p=plan(now);out.mkdir(parents=True,exist_ok=False);save(out/'plan.json',p)
    client=MarketauxClient(token,max_requests=p['maxRequests']);results=[];articles={};stop=None
    try:
        todo=list(p['queries'])
        while todo:
            q=todo.pop(0)
            try:
                response=client.get(q['endpoint'],q['parameters']);save(out/(q['case']+'.json'),response)
                if q['endpoint']=='news/all':
                    checked=assessment(response['data'],q['parameters'],now=datetime.now(timezone.utc))
                    checked['expectedSymbol']=q.get('expectedSymbol')
                    checked['expectedSymbolMentions']=sum(any(e.get('symbol')==q.get('expectedSymbol') for e in a['entities']) for a in response['data']) if q.get('expectedSymbol') else None
                    checked['status']='RETURNED_SAMPLE' if response['data'] else 'EMPTY_SAMPLE_UNCONFIRMED_COVERAGE'
                    for a in response['data']:articles.setdefault(a['uuid'],a)
                    if q.get('paginate') and len(response['data'])==3 and isinstance(response['meta'].get('found'),int) and response['meta']['found']>3:
                        todo.insert(0,dict(**{k:v for k,v in q.items() if k not in {'case','parameters','paginate'}},case=q['case']+'_page2',parameters={**q['parameters'],'page':2}))
                else:
                    matched=[e for e in response['data'] if e.get('symbol')==q['expectedSymbol'] and e.get('country')=='us' and e.get('type')=='equity']
                    checked=dict(status='ENTITY_EXACT_UNIQUE' if len(matched)==1 else 'ENTITY_REVIEW_REQUIRED',returned=len(response['data']),exactIdentityCandidates=len(matched))
                results.append(dict(case=q['case'],endpoint=q['endpoint'],http=200,seconds=response['seconds'],meta=response['meta'],assessment=checked))
            except MarketauxError as e:
                item=dict(case=q['case'],status='REQUEST_HELD',code=e.code,http=e.status,stop=e.stop)
                save(out/(q['case']+'-error.json'),item);results.append(item)
                if e.stop:stop=e.code;break
    finally:client.close()
    save(out/'articles.json',list(articles.values()));save(out/'case-results.json',results)
    good=[r for r in results if r.get('http')==200];total=sum(r.get('assessment',{}).get('returned',0) for r in good if r['endpoint']=='news/all')
    summary=dict(status='HELD' if stop else 'BOUNDED_EVALUATION_FINISHED',stopReason=stop,requestsAttempted=client.attempts,plannedQueriesNotRun=[q['case'] for q in todo],cases=len(results),successfulRequests=len(good),uniqueArticles=len(articles),newsArticleAppearances=total,duplicateArticleAppearances=total-len(articles),
                 manualRelevanceReviewPending=True,allHistoryCoverageGuaranteed=False,publicRedistributionApproved=False,paidPlanActivated=False,awsChanges=0,dbWrites=0,modelCalls=0,sourcePagesFetched=0)
    save(out/'summary.json',summary);print(json.dumps(summary,ensure_ascii=True));return summary

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--run',action='store_true');args=parser.parse_args()
    now=datetime.now(timezone.utc);p=plan(now)
    keyfile=ROOT/'trader-data/.env.news-api'
    token=dotenv_values(keyfile,encoding='utf-8-sig').get(TOKEN_ENV,'') if keyfile.exists() else ''
    if not args.run:print(json.dumps(dict(tokenConfigured=bool(token and len(token)>=12),keyFile=str(keyfile),variable=TOKEN_ENV,maximumRequests=p['maxRequests'],plannedMaximum=p['plannedMaximum'],cases=len(p['queries']),networkRequests=0)))
    else:
        if not token:raise SystemExit('MARKETAUX_TOKEN_NOT_CONFIGURED')
        run(ROOT/'output'/('news-marketaux-eval-'+now.strftime('%Y%m%dT%H%M%SZ')),token,now)
