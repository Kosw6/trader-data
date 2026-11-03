"""Bounded Marketaux reads. No hidden retries, redirects, article crawling or DB writes."""
import hashlib,json,math,re,time
from datetime import datetime,timezone
from urllib.parse import urlsplit,urlunsplit,parse_qsl,urlencode
import requests
from requests.adapters import HTTPAdapter

BASE='https://api.marketaux.com/v1'
TOKEN_ENV='TRADER_NEWS_MARKETAUX_API_TOKEN'
ARTICLE_FIELDS={'uuid','title','description','snippet','url','language','published_at','source','relevance_score'}
ENTITY_FIELDS={'symbol','name','exchange','exchange_long','country','type','industry','match_score','sentiment_score'}
SECRET_PARAMS={'api_token','token','access_token','apikey','api_key','key','authorization'}

class MarketauxError(RuntimeError):
    def __init__(self,code,status=None,stop=False):
        self.code=code;self.status=status;self.stop=stop
        super().__init__(code) # Never include a request URL, token, or response message.

def utc(value):
    d=datetime.fromisoformat(value.replace('Z','+00:00'))
    if d.tzinfo is None:d=d.replace(tzinfo=timezone.utc) # Marketaux dates are UTC.
    return d.astimezone(timezone.utc)

def safe_url(value):
    if not isinstance(value,str):return None
    try:
        u=urlsplit(value)
        if u.scheme not in {'https','http'} or not u.hostname or u.username or u.password:return None
        params=[(k,v) for k,v in parse_qsl(u.query,keep_blank_values=True) if k.casefold() not in SECRET_PARAMS]
        return urlunsplit((u.scheme,u.netloc,u.path,urlencode(params),''))
    except ValueError:return None

def sanitized(value,secret):
    if isinstance(value,str):
        value=value.replace(secret,'[REDACTED]') if secret else value
        return re.sub(r'(?i)((?:api_token|api_key|apikey|access_token|authorization|token)\s*[=:]\s*)[^\s&"<>]+',r'\1[REDACTED]',value)
    if isinstance(value,list):return [sanitized(x,secret) for x in value]
    if isinstance(value,dict):return {k:sanitized(v,secret) for k,v in value.items() if k.casefold() not in SECRET_PARAMS}
    if value is None or isinstance(value,(bool,int)):return value
    if isinstance(value,float) and math.isfinite(value):return value
    return None

def normalize_article(article,secret=''):
    if not isinstance(article,dict):raise MarketauxError('NEWS_ARTICLE_SCHEMA_INVALID',stop=True)
    if not isinstance(article.get('uuid'),str) or not article['uuid'] or not isinstance(article.get('title'),str) or not article['title'].strip():
        raise MarketauxError('NEWS_ARTICLE_ID_TITLE_REQUIRED',stop=True)
    if not safe_url(article.get('url')):raise MarketauxError('NEWS_ARTICLE_URL_INVALID',stop=True)
    try:published=utc(article['published_at'])
    except (ValueError,KeyError,TypeError,AttributeError):raise MarketauxError('NEWS_ARTICLE_DATE_INVALID',stop=True) from None
    entities=article.get('entities',[])
    if not isinstance(entities,list) or len(entities)>500:raise MarketauxError('NEWS_ENTITY_SCHEMA_INVALID',stop=True)
    item={k:article.get(k) for k in ARTICLE_FIELDS};item['url']=safe_url(item['url']);item['published_at']=published.isoformat().replace('+00:00','Z')
    item['entities']=[{k:e.get(k) for k in ENTITY_FIELDS} for e in entities if isinstance(e,dict)]
    similar=article.get('similar') or []
    if not isinstance(similar,list):raise MarketauxError('NEWS_SIMILAR_SCHEMA_INVALID',stop=True)
    item['similar_ids']=[a['uuid'] for a in similar if isinstance(a,dict) and isinstance(a.get('uuid'),str)][:100]
    item['provider']='MARKETAUX';item['content_kind']='PROVIDER_METADATA_AND_EXCERPTS'
    # Neither a provider article timestamp nor sentiment proves event timing or causality.
    item['event_date']=None;item['causal_stock_impact_verified']=False
    return sanitized(item,secret)

def assessment(articles,params,now=None):
    now=now or datetime.now(timezone.utc);lower=utc(params['published_after']) if params.get('published_after') else None
    upper=utc(params['published_before']) if params.get('published_before') else None
    symbols=set(params.get('symbols','').split(','))-{''};bad_dates=[];missing_symbols=[]
    for a in articles:
        d=utc(a['published_at'])
        if (lower and d<lower) or (upper and d>upper) or d>now:bad_dates.append(a['uuid'])
        if symbols and not symbols.intersection(e.get('symbol') for e in a['entities']):missing_symbols.append(a['uuid'])
    ids=[a['uuid'] for a in articles];urls=[a['url'] for a in articles]
    return dict(returned=len(articles),dateViolations=bad_dates,symbolAssociationMissing=missing_symbols,
                duplicateIds=len(ids)-len(set(ids)),duplicateUrls=len(urls)-len(set(urls)),
                publisherCount=len({a.get('source') for a in articles}),
                minimumDate=min((a['published_at'] for a in articles),default=None),maximumDate=max((a['published_at'] for a in articles),default=None),
                associationVerified=bool(articles) and bool(symbols) and not missing_symbols,
                relevanceManuallyReviewed=False,archiveCompletenessVerified=False)

class MarketauxClient:
    def __init__(self,token,*,max_requests=20,session=None,min_interval=1.05):
        if not isinstance(token,str) or not re.fullmatch(r'[A-Za-z0-9_.\-]{12,512}',token):raise MarketauxError('MARKETAUX_TOKEN_MISSING_OR_INVALID',stop=True)
        if type(max_requests) is not int or not 1<=max_requests<=20:raise ValueError('NEWS_EXPERIMENT_REQUEST_CAP')
        self.__token=token;self.max_requests=max_requests;self.attempts=0;self.last=0.;self.min_interval=min_interval
        self.session=session or requests.Session();self.session.trust_env=False
        self.session.mount('https://',HTTPAdapter(max_retries=0));self.session.headers.update({'User-Agent':'RevestLab-News-Evaluation/1.0','Accept':'application/json'})
    def __repr__(self):return 'MarketauxClient(token=[REDACTED], attempts='+str(self.attempts)+')'
    def close(self):self.session.close()
    def get(self,endpoint,params):
        if endpoint not in {'news/all','entity/search'}:raise ValueError('MARKETAUX_ENDPOINT_NOT_ALLOWED')
        permitted={'symbols','search','countries','types','entity_types','filter_entities','must_have_entities','group_similar','language','published_after','published_before','published_on','limit','page','sort','sort_order'}
        if set(params)-permitted or any(k.casefold() in SECRET_PARAMS for k in params):raise ValueError('NEWS_QUERY_NOT_ALLOWED')
        if endpoint=='news/all' and (type(params.get('limit')) is not int or not 1<=params['limit']<=3):raise ValueError('NEWS_FREE_PLAN_ARTICLE_CAP')
        if self.attempts>=self.max_requests:raise MarketauxError('LOCAL_REQUEST_CAP_REACHED',stop=True)
        time.sleep(max(0,self.min_interval-(time.monotonic()-self.last)));self.last=time.monotonic();self.attempts+=1
        started=time.monotonic()
        try:
            with self.session.get(BASE+'/'+endpoint,params={**params,'api_token':self.__token},timeout=(5,20),allow_redirects=False,stream=True) as response:
                status=response.status_code
                if status!=200:
                    # Server error messages can echo credentials. Persist only fixed codes.
                    code={400:'MARKETAUX_PARAMETERS_REJECTED',401:'MARKETAUX_AUTH_REJECTED',402:'MARKETAUX_QUOTA_EXHAUSTED',403:'MARKETAUX_PLAN_RESTRICTED',404:'MARKETAUX_RESOURCE_MISSING',429:'MARKETAUX_RATE_LIMIT',500:'MARKETAUX_SERVER_ERROR',503:'MARKETAUX_MAINTENANCE'}.get(status,'MARKETAUX_HTTP_REJECTED')
                    raise MarketauxError(code,status,stop=status in {401,402,429} or status>=500 or 300<=status<400)
                body=bytearray()
                for b in response.iter_content(65536):
                    body.extend(b)
                    if len(body)>2*1024**2:raise MarketauxError('NEWS_RESPONSE_SIZE_LIMIT',stop=True)
                h={k:v for k,v in response.headers.items() if k.casefold() in {'x-usagelimit-limit','x-usagelimit-remaining','x-ratelimit-limit','x-ratelimit-remaining'}}
                try:payload=json.loads(body)
                except (ValueError,UnicodeError):raise MarketauxError('NEWS_JSON_INVALID',stop=True) from None
                if not isinstance(payload,dict) or 'error' in payload or not isinstance(payload.get('data'),list):raise MarketauxError('NEWS_RESPONSE_SCHEMA_INVALID',stop=True)
                if endpoint=='news/all':
                    if len(payload['data'])>params['limit']:raise MarketauxError('NEWS_RESPONSE_ARTICLE_CAP',stop=True)
                    data=[normalize_article(a,self.__token) for a in payload['data']]
                else:data=sanitized([{k:e.get(k) for k in ENTITY_FIELDS} for e in payload['data'] if isinstance(e,dict)],self.__token)
                meta=payload.get('meta',{});meta={k:meta.get(k) for k in ['found','returned','limit','page']} if isinstance(meta,dict) else {}
                return dict(provider='MARKETAUX',endpoint=endpoint,parameters=dict(params),http=status,attempt=self.attempts,
                            seconds=round(time.monotonic()-started,3),received_at=datetime.now(timezone.utc).isoformat(),
                            response_sha256=hashlib.sha256(body).hexdigest(),response_bytes=len(body),usageHeaders=sanitized(h,self.__token),
                            meta=meta,data=data)
        except requests.RequestException:raise MarketauxError('MARKETAUX_TRANSPORT_ERROR',stop=True) from None
