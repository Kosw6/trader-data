import json
from datetime import datetime,timezone
import pytest,requests
from collectors.marketaux_client import MarketauxClient,MarketauxError,normalize_article,assessment,safe_url

SECRET='test_credential_never_print_12345'

def article(**changes):
    return dict(uuid='article-1',title='NVIDIA announces results',description='Company report.',snippet='Example excerpt.',url='https://example.org/news/item',language='en',published_at='2026-10-01T13:00:00.000Z',source='example.org',entities=[dict(symbol='NVDA',name='NVIDIA Corporation',country='us',type='equity')],similar=[],**changes)

class Response:
    def __init__(self,payload,status=200):self.payload=payload;self.status_code=status;self.headers={'X-UsageLimit-Limit':'100','Authorization':SECRET}
    def __enter__(self):return self
    def __exit__(self,*a):pass
    def iter_content(self,n):yield json.dumps(self.payload).encode()

class Session:
    def __init__(self,response):self.response=response;self.calls=[];self.headers={};self.closed=False
    def mount(self,*a):pass
    def get(self,*a,**k):
        self.calls.append((a,k))
        if isinstance(self.response,Exception):raise self.response
        return self.response
    def close(self):self.closed=True

def client(response,max_requests=20):
    s=Session(response);c=MarketauxClient(SECRET,session=s,max_requests=max_requests,min_interval=0);return c,s

PARAMS=dict(symbols='NVDA',limit=3,page=1)

def test_auth_only_goes_to_fixed_provider_and_saved_output_contains_no_key():
    data=article();data['title']='headline '+SECRET;data['url']='https://example.org/a?api_token='+SECRET+'&story=1';data['irrelevant_secret']=SECRET
    c,s=client(Response({'data':[data],'meta':dict(found=1,returned=1,page=1,limit=3)}))
    result=c.get('news/all',PARAMS)
    assert s.calls[0][0]==('https://api.marketaux.com/v1/news/all',)
    assert s.calls[0][1]['params']['api_token']==SECRET and not s.calls[0][1]['allow_redirects']
    assert s.calls[0][1]['timeout']==(5,20) and not s.trust_env
    assert SECRET not in json.dumps(result) and SECRET not in repr(c)
    assert result['data'][0]['url']=='https://example.org/a?story=1'
    assert result['data'][0]['event_date'] is None and result['data'][0]['causal_stock_impact_verified'] is False
    assert set(result['usageHeaders'])=={'X-UsageLimit-Limit'}
    c.close();assert s.closed

@pytest.mark.parametrize('status,stop',[(301,True),(400,False),(401,True),(402,True),(403,False),(404,False),(429,True),(500,True),(503,True)])
def test_errors_never_retry_or_echo_tokens(status,stop):
    c,s=client(Response({'error':{'code':'bad','message':'api_token='+SECRET}},status))
    with pytest.raises(MarketauxError) as e:c.get('news/all',PARAMS)
    assert e.value.status==status and e.value.stop==stop and SECRET not in str(e.value)
    assert len(s.calls)==c.attempts==1

def test_no_hidden_network_retry_on_transport_failure():
    c,s=client(requests.ConnectionError('https://api.marketaux.com?api_token='+SECRET))
    with pytest.raises(MarketauxError,match='MARKETAUX_TRANSPORT_ERROR'):c.get('news/all',PARAMS)
    assert len(s.calls)==1

def test_request_cap_counts_failures_too():
    c,s=client(Response({},400),max_requests=1)
    with pytest.raises(MarketauxError):c.get('news/all',PARAMS)
    with pytest.raises(MarketauxError,match='LOCAL_REQUEST_CAP_REACHED'):c.get('news/all',PARAMS)
    assert len(s.calls)==1

@pytest.mark.parametrize('endpoint,params',[
 ('https://elsewhere.invalid',PARAMS),('news/all',dict(limit=20)),('news/all',dict(limit=3,api_token='replacement')),
 ('news/all',dict(limit=3,url='https://elsewhere.invalid'))])
def test_unbounded_or_arbitrary_requests_rejected_without_network(endpoint,params):
    c,s=client(Response({}))
    with pytest.raises(ValueError):c.get(endpoint,params)
    assert not s.calls and c.attempts==0

def test_article_schema_or_unexpected_response_cannot_silently_pass():
    for payload in [{'data':{}},{'data':[],'error':{'message':SECRET}},{'data':[article()]*4}]:
        c,_=client(Response(payload))
        with pytest.raises(MarketauxError):c.get('news/all',PARAMS)
    a=article();a['published_at']='bad'
    with pytest.raises(MarketauxError):normalize_article(a)

def test_wrong_symbol_outside_date_and_duplicates_are_not_claimed_correct():
    a=normalize_article(article());p=dict(symbols='AAPL',published_after='2026-10-02T00:00:00',published_before='2026-11-01T00:00:00')
    r=assessment([a,a],p,now=datetime(2026,11,1,tzinfo=timezone.utc))
    assert r['symbolAssociationMissing']==['article-1']*2 and r['dateViolations']==['article-1']*2
    assert r['duplicateIds']==r['duplicateUrls']==1 and not r['associationVerified']
    assert not r['relevanceManuallyReviewed'] and not r['archiveCompletenessVerified']

def test_empty_history_does_not_mean_confirmed_no_news_or_full_archive():
    r=assessment([],dict(symbols='NVDA',published_after='2016-01-01',published_before='2016-12-31'))
    assert r['returned']==0 and not r['associationVerified'] and not r['archiveCompletenessVerified']

@pytest.mark.parametrize('url',['javascript:alert(1)','file:///a','https://user:password@example.org/a'])
def test_unsafe_source_urls_rejected(url):assert safe_url(url) is None

def test_real_adapter_has_zero_automatic_retries():
    c=MarketauxClient(SECRET);assert c.session.get_adapter('https://').max_retries.total==0;c.close()
