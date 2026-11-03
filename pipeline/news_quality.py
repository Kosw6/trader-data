"""Local metadata quality labels, not truth checks or investment signals."""
import hashlib,json,re
from datetime import datetime,timezone

def company_relevance(article,symbol,aliases):
    """Exact provider identity is necessary; incidental mentions stay separate."""
    bound=any(e.get('symbol')==symbol and e.get('type')=='equity' and e.get('country')=='us' for e in article.get('entities',[]))
    pattern=re.compile(r'(?<![A-Za-z0-9])(?:'+'|'.join(re.escape(s) for s in [symbol,*aliases])+r')(?![A-Za-z0-9])',re.I)
    title=bool(pattern.search(article.get('title') or ''))
    excerpt=bool(pattern.search(' '.join(article.get(k) or '' for k in ['description','snippet'])))
    category='ENTITY_REVIEW_REQUIRED' if not bound else 'TITLE_MENTION' if title else 'EXCERPT_MENTION' if excerpt else 'PROVIDER_ASSOCIATION_ONLY'
    commentary=bool(re.search(r'\bprediction\b|\bovervalued\b|\bundervalued\b|\bDCF\b|\bbull\b|\bbear\b|\bP/E\b',article.get('title') or '',re.I))
    return dict(symbol=symbol,providerUsEquityIdentity=bound,category=category,
                primaryFeedCandidate=bound and title,relatedFeedCandidate=bound and not title,
                commentaryTitleHint=commentary,verifiedFact=False,verifiedPriceCause=False,
                explanation='Title/excerpt matching only. Relevance is not editorial quality, confirmed truth, or causal stock impact.')

def merge_observations(observations):
    """Keep per-query provenance and union entities without losing other tickers.

    Same UUID with conflicting core metadata is quarantined, not silently
    overwritten. Different article UUIDs with similar titles are not collapsed.
    """
    result={}
    for case,article in observations:
        uid=article['uuid']
        if uid not in result:
            result[uid]=dict(article,entities=list(article.get('entities',[])),observedInCases=[],metadataConflicts=[])
        row=result[uid];row['observedInCases'].append(case)
        for k in ['title','url','published_at','language']:
            if article.get(k)!=row.get(k):row['metadataConflicts'].append(dict(case=case,field=k,alternative=article.get(k)))
        known={json.dumps(e,sort_keys=True) for e in row['entities']}
        for e in article.get('entities',[]):
            key=json.dumps(e,sort_keys=True)
            if key not in known:row['entities'].append(e);known.add(key)
    for row in result.values():
        row['observedInCases']=sorted(set(row['observedInCases']))
        row['metadataConsistent']=not row['metadataConflicts']
        row['event_date']=None;row['causal_stock_impact_verified']=False
    return sorted(result.values(),key=lambda a:(a['published_at'],a['uuid']),reverse=True)
