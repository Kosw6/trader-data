"""Experimental public RSS, not a guaranteed historical news API or article body feed."""
import html
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from collectors.marketaux_client import safe_url

URL='https://news.google.com/rss/search'


def parse_feed(raw,year=2016,sample_limit=20):
    if len(raw)>1024**2 or re.search(br'<!DOCTYPE|<!ENTITY',raw,re.I):
        raise ValueError('RSS_XML_SIZE_OR_ENTITY_NOT_ALLOWED')
    try:root=ET.fromstring(raw)
    except ET.ParseError:raise ValueError('RSS_XML_INVALID') from None
    if root.tag!='rss':raise ValueError('RSS_SCHEMA_INVALID')
    items=root.findall('./channel/item')
    if len(items)>200:raise ValueError('RSS_ITEM_CAP')
    articles=[];invalid=0;out_of_year=0;excluded=[]
    for item in items:
        try:
            title=html.unescape(item.findtext('title') or '')
            link=safe_url(item.findtext('link'))
            published=parsedate_to_datetime(item.findtext('pubDate') or '')
            if not title or not link or published.tzinfo is None:raise ValueError()
            if year is not None and published.year!=year:
                out_of_year+=1
                if len(excluded)<5:excluded.append(dict(title=title,reportedDate=published.isoformat(),reason='OUTSIDE_REQUESTED_YEAR'))
                continue
            source=item.find('source')
            articles.append(dict(title=title,aggregatorUrl=link,
                 publisher=source.text if source is not None else None,
                 publisherHome=safe_url(source.attrib.get('url')) if source is not None else None,
                 aggregatorReportedPublishedAt=published.isoformat(),publishedAtIndependentlyVerified=False,
                 body=None,eventDate=None,originalArticleVerified=False,
                 relevance='KEYWORD_MATCH_CANDIDATE_NOT_VERIFIED_TICKER_BINDING'))
        except (TypeError,ValueError,AttributeError,OverflowError):invalid+=1
    return dict(provider='GOOGLE_NEWS_RSS_EXPERIMENTAL',returnedItems=len(items),
                matchingYearItems=len(articles),invalidItems=invalid,outOfYearItems=out_of_year,
                uniqueLinks=len({a['aggregatorUrl'] for a in articles}),
                samples=articles if sample_limit is None else articles[:sample_limit],excludedDateSamples=excluded,
                allHistoryCoverageVerified=False,bodyAvailable=False)
