from pipeline.news_quality import company_relevance,merge_observations

def article(uid='1',title='NVIDIA announces results',symbol='NVDA'):
    return dict(uuid=uid,title=title,url='https://example.org/'+uid,published_at='2026-11-01T01:00:00Z',language='en',description='Market report.',snippet='',entities=[dict(symbol=symbol,type='equity',country='us')])

def test_ticker_association_alone_is_not_a_central_company_article():
    a=article(title='A general market update')
    r=company_relevance(a,'NVDA',['NVIDIA'])
    assert r['category']=='PROVIDER_ASSOCIATION_ONLY' and not r['primaryFeedCandidate'] and r['relatedFeedCandidate']
    assert not r['verifiedFact'] and not r['verifiedPriceCause']

def test_title_match_without_entity_binding_needs_review():
    r=company_relevance(article(symbol='OTHER'),'NVDA',['NVIDIA'])
    assert r['category']=='ENTITY_REVIEW_REQUIRED' and not r['primaryFeedCandidate']

def test_same_article_keeps_all_target_entities_and_query_provenance():
    a=article();b=article(symbol='AAPL');rows=merge_observations([('NVDA',a),('AAPL',b)])
    assert len(rows)==1 and {e['symbol'] for e in rows[0]['entities']}=={'NVDA','AAPL'}
    assert rows[0]['observedInCases']==['AAPL','NVDA'] and rows[0]['metadataConsistent']

def test_conflicting_same_uuid_is_quarantined_and_distinct_ids_are_not_merged():
    a=article();b=article(title='Changed title');c=article('2')
    rows=merge_observations([('a',a),('b',b),('c',c)])
    assert len(rows)==2
    assert not next(r for r in rows if r['uuid']=='1')['metadataConsistent']

def test_forecast_headline_is_not_a_stock_price_fact():
    r=company_relevance(article(title='NVIDIA stock prediction: bull and bear'),'NVDA',['NVIDIA'])
    assert r['primaryFeedCandidate'] and r['commentaryTitleHint'] and not r['verifiedFact']
