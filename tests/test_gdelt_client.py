import io
import json
import zipfile
import pytest
import requests
from collectors.gdelt_client import GdeltClient,GdeltError,DOC,parse_doc,parse_gkg,labels

PARAMS=dict(query='Nvidia',mode='artlist',format='json',maxrecords=5)


def zipped(fields,name='20161214200000.gkg.csv'):
    stream=io.BytesIO()
    with zipfile.ZipFile(stream,'w') as z:z.writestr(name,'\t'.join(fields)+'\n')
    return stream.getvalue()


def row():
    a=['']*27;a[0]='20161214200000-1';a[1]='20161214200000';a[2]='1';a[3]='example.org'
    a[4]='https://example.org/news';a[7]='ECON_INTEREST_RATES;';a[13]='nvidia;federal reserve;'
    return a


def test_gkg_snapshot_time_never_becomes_publication_or_event_time():
    r=parse_gkg(zipped(row()),'20161214200000')
    assert r['rows']==1 and r['invalidRows']==0
    a=r['samples'][0]
    assert a['candidateLabels']==['NVDA','MACRO_RATES_CANDIDATE']
    assert a['providerDocumentDateRaw']=='20161214200000' and a['gdeltBatchAt']=='20161214200000'
    assert a['publishedAt'] is None and a['eventDate'] is None and a['body'] is None
    assert not a['originalArticleVerified'] and not r['fullYearCoverageVerified']


def test_no_zip_extraction_or_arbitrary_members():
    with pytest.raises(GdeltError,match='GKG_ZIP_MEMBER_OR_SIZE_INVALID'):
        parse_gkg(zipped(row(),'../../other'),'20161214200000')
    with pytest.raises(GdeltError,match='GKG_NOT_A_ZIP'):parse_gkg(b'not a zip','20161214200000')


def test_malformed_columns_or_unsafe_url_quarantined():
    a=row();a[4]='file:///a'
    r=parse_gkg(zipped(a),'20161214200000')
    assert r['invalidRows']==1 and not r['samples']
    a=row();a[1]='20169999999999'
    r=parse_gkg(zipped(a),'20161214200000')
    assert r['invalidRows']==1 and not r['samples']


def test_doc_timestamp_is_seen_time_and_publisher_country_not_event_country():
    raw=json.dumps({'articles':[dict(url='https://example.org/a',title='Nvidia &amp; AI',seendate='20261101T120000Z',domain='example.org',sourcecountry='United States')]}).encode()
    a=parse_doc(raw)[0]
    assert a['title']=='Nvidia & AI' and a['seenAt'].startswith('2026-11-01')
    assert a['publishedAt'] is None and a['publisherCountry']=='United States'


def test_rejected_or_empty_query_cannot_be_reported_as_archive_completeness():
    with pytest.raises(GdeltError,match='DOC_DATE_RANGE_REJECTED'):
        parse_doc(b'Your query start date is outside the available time range.')
    assert parse_doc(b'{"articles":[]}')==[]
    with pytest.raises(GdeltError,match='DOC_RECORD_CAP_NOT_HONORED'):
        parse_doc(json.dumps({'articles':[{}]*6}).encode())


def test_local_organization_candidate_matching_is_not_ticker_substring_guessing():
    assert labels(['pineapple company','tesla motors'],[])==['TSLA']
    assert labels(['apple inc','NVIDIA'],[])==['NVDA','AAPL']


def test_fixed_hosts_caps_and_zero_automatic_retry():
    c=GdeltClient(interval=0)
    assert c.session.get_adapter('https://').max_retries.total==0 and not c.session.trust_env
    with pytest.raises(ValueError):c.get('https://other.example/a')
    with pytest.raises(ValueError):c.get(DOC,{**PARAMS,'api_key':'something'})
    c.attempts=12
    with pytest.raises(GdeltError,match='LOCAL_REQUEST_CAP'):c.get(DOC,PARAMS)
    c.close()
