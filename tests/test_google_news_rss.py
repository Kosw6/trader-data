import pytest
from collectors.google_news_rss import parse_feed


def feed(date='Fri, 30 Dec 2016 13:05:00 GMT'):
    return ('<rss><channel><item><title>NVIDIA &amp; CES - Example</title>'
            '<link>https://news.google.com/rss/articles/example</link>'
            '<source url="https://example.org">Example</source>'
            '<pubDate>'+date+'</pubDate></item></channel></rss>').encode()


def test_historical_rss_date_is_provider_reported_not_independently_verified():
    r=parse_feed(feed());a=r['samples'][0]
    assert r['matchingYearItems']==1 and a['title']=='NVIDIA & CES - Example'
    assert not a['publishedAtIndependentlyVerified'] and a['body'] is None
    assert not r['allHistoryCoverageVerified']


def test_search_ignoring_requested_year_is_explicit_not_counted_as_2016():
    r=parse_feed(feed('Sun, 01 Nov 2026 13:05:00 GMT'))
    assert r['outOfYearItems']==1 and r['matchingYearItems']==0 and not r['samples']


def test_xml_entities_invalid_documents_and_oversized_responses_rejected():
    for raw in [b'<!DOCTYPE rss><rss/>',b'<rss',b'x'*(1024**2+1)]:
        with pytest.raises(ValueError):parse_feed(raw)
