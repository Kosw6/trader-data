"""One reserved request: compare keyword default relevance with explicit date sort."""
import json,sys
from pathlib import Path
from datetime import datetime,timezone
from dotenv import dotenv_values
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'trader-data'))
from collectors.marketaux_client import MarketauxClient,MarketauxError,TOKEN_ENV,assessment
from experiments.marketaux_news_eval import save
if __name__=='__main__':
    p=ROOT/'output/news-marketaux-eval-20261101T111108Z';summary=json.loads((p/'summary.json').read_text())
    assert summary['requestsAttempted']==19 and summary['status']=='BOUNDED_EVALUATION_FINISHED'
    intent=p/'latest-sort-probe-intent.json'
    assert not intent.exists(),'PROBE_INTENT_EXISTS_DO_NOT_RETRY'
    before=json.loads((p/'name_NVDA.json').read_text(encoding='utf8'));params={**before['parameters'],'sort':'published_at'}
    token=dotenv_values(ROOT/'trader-data/.env.news-api',encoding='utf-8-sig').get(TOKEN_ENV,'')
    c=MarketauxClient(token,max_requests=1)
    save(intent,dict(status='ATTEMPT_INTENDED',globalRequestCeiling=20,priorRequests=19,parameters=params))
    try:
        response=c.get('news/all',params);save(p/'name_NVDA_latest.json',response)
        checked=assessment(response['data'],params)
        result=dict(status='RETURNED_SAMPLE',totalRequests=19+c.attempts,beforeMaximumDate=max((a['published_at'] for a in before['data']),default=None),afterMaximumDate=checked['maximumDate'],assessment=checked,usageHeaders=response['usageHeaders'])
    except MarketauxError as e:result=dict(status='HELD',code=e.code,http=e.status,totalRequests=19+c.attempts)
    finally:c.close()
    save(p/'latest-sort-probe-result.json',result);print(json.dumps(result))
