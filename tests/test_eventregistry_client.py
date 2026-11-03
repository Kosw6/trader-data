import json
from datetime import datetime, timezone

import pytest
import requests

from collectors.eventregistry_client import (ARTICLES, SUGGEST, NewsApiClient,
    NewsApiError, article_params, assess, normalize_article, archive_assessment)
from experiments.eventregistry_news_eval import choose_concept

KEY = 'local_test_key_not_a_real_credential_123'
PARAMS = article_params('2016-01-01', '2016-12-31', concept='org:nvidia', historical=True)


def article(**changes):
    return dict(uri='article1', title='Nvidia earnings', date='2016-12-01',
                dateTime='2016-12-01T12:00:00Z', url='https://example.org/a',
                body='x' * 500, source={'uri': 'example.org', 'title': 'Example'},
                concepts=[dict(uri='org:nvidia', type='org', label={'eng': 'Nvidia'})], **changes)


class Response:
    def __init__(self, payload, status=200):
        self.payload, self.status_code = payload, status
        self.headers = {'req-tokens': '5', 'req-archive': '1', 'authorization': KEY}
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def iter_content(self, size): yield json.dumps(self.payload).encode()


class Session:
    def __init__(self, response):
        self.response, self.calls, self.headers = response, [], {}
    def mount(self, *args): pass
    def post(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if isinstance(self.response, Exception): raise self.response
        return self.response
    def close(self): pass


def client(payload=None, status=200, **kwargs):
    s = Session(Response(payload if payload is not None else {'articles': {'results': [article()]}}, status))
    return NewsApiClient(KEY, session=s, min_interval=0, **kwargs), s


def test_fixed_https_post_no_query_key_no_retries_and_redacted_receipt():
    a = article()
    a['title'] += KEY
    a['url'] += '?apiKey=' + KEY
    c, s = client({'articles': {'results': [a], 'totalResults': 1}})
    result = c.post(ARTICLES, PARAMS)
    args, kw = s.calls[0]
    assert args == ('https://eventregistry.org/api/v1/article/getArticles',)
    assert kw['json']['apiKey'] == KEY and not kw['allow_redirects']
    assert 'params' not in kw and not s.trust_env
    assert KEY not in json.dumps(result) and KEY not in repr(c)
    assert len(result['data'][0]['excerpt']) == 400
    assert result['data'][0]['event_date'] is None
    assert c.attempts == 1 and c.reported_tokens == 5


@pytest.mark.parametrize('status', [301, 400, 401, 403, 429, 500, 503])
def test_failure_never_retries_or_echoes_request(status):
    c, s = client({'error': 'apiKey=' + KEY}, status)
    with pytest.raises(NewsApiError) as e: c.post(ARTICLES, PARAMS)
    assert KEY not in str(e.value) and KEY not in json.dumps(e.value.receipt)
    assert e.value.stop and not e.value.archive_denied and len(s.calls) == 1


def test_known_archive_plan_restriction_is_not_no_news_or_auth_failure():
    c, _ = client({'error': 'Your plan does not allow access to archive data'}, 403)
    with pytest.raises(NewsApiError) as e: c.post(ARTICLES, PARAMS)
    assert e.value.archive_denied and not e.value.stop
    assert e.value.code == 'ARCHIVE_ACCESS_REJECTED'


def test_transport_exception_does_not_expose_credential():
    c, s = client()
    s.response = requests.ConnectionError('apiKey=' + KEY)
    with pytest.raises(NewsApiError) as e: c.post(ARTICLES, PARAMS)
    assert str(e.value) == 'NEWSAPI_TRANSPORT_ERROR' and len(s.calls) == 1


@pytest.mark.parametrize('endpoint,change', [
    ('https://elsewhere.invalid', {}), (ARTICLES, {'apiKey': KEY}),
    (ARTICLES, {'articlesPage': 2}), (ARTICLES, {'articlesCount': 100}),
    (ARTICLES, {'articleBodyLen': -1}), (ARTICLES, {'dateEnd': '2017-12-31'}),
    (ARTICLES, {'url': 'https://elsewhere.invalid'})])
def test_unsafe_or_unbounded_query_rejected_before_network(endpoint, change):
    c, s = client()
    with pytest.raises(ValueError): c.post(endpoint, {**PARAMS, **change})
    assert not s.calls and c.attempts == 0


def test_request_and_conservative_reservation_caps_include_failures():
    c, s = client(max_requests=1)
    c.post(ARTICLES, PARAMS)
    with pytest.raises(NewsApiError, match='LOCAL_EXPERIMENT_CAP_REACHED'): c.post(ARTICLES, PARAMS)
    assert len(s.calls) == 1
    c, s = client(reservation_cap=4)
    with pytest.raises(NewsApiError): c.post(ARTICLES, PARAMS)
    assert not s.calls


def test_dates_and_entity_binding_fail_closed_without_coverage_claims():
    a = normalize_article(article(), KEY)
    checked = assess([a, a], {**PARAMS, 'dateStart': '2016-12-02', 'conceptUri': 'org:other'})
    assert checked['dateViolations'] == ['article1', 'article1']
    assert checked['conceptAssociationMissing'] == ['article1', 'article1']
    assert checked['duplicateIds'] == 1 and not checked['archiveCompletenessVerified']
    assert not assess([], PARAMS)['archiveCompletenessVerified']


def test_company_suggestion_does_not_choose_ambiguous_or_wrong_type():
    a = dict(uri='org:nvidia', type='org', label={'eng': 'Nvidia'})
    assert choose_concept([a], {'nvidia'}) == 'org:nvidia'
    assert choose_concept([a, {**a, 'uri': 'org:other'}], {'nvidia'}) is None
    assert choose_concept([{**a, 'type': 'wiki'}], {'nvidia'}) is None


def test_malformed_article_not_accepted():
    for field, value in [('date', 'bad'), ('url', 'file:///etc/passwd'), ('concepts', {}), ('source', [])]:
        a = article(); a[field] = value
        with pytest.raises(NewsApiError): normalize_article(a, KEY)


def test_real_http_adapter_has_zero_retries():
    c = NewsApiClient(KEY)
    assert c.session.get_adapter('https://').max_retries.total == 0
    c.close()


def test_decimal_usage_headers_are_actual_billing_values_not_missing():
    c, s = client()
    s.response.headers['req-tokens'] = '1.000'
    c.post(ARTICLES, PARAMS)
    assert c.tokens_known and c.reported_tokens == 1
    s.response.headers['req-tokens'] = '0.500'
    c.post(ARTICLES, PARAMS)
    assert c.reported_tokens == 1.5


def test_localized_organization_suggestions_resolve_unique_english_uri():
    candidates = [dict(uri=u, type='org', label={'eng': 'Nvidia'}) for u in
                  ['http://en.wikipedia.org/wiki/Nvidia', 'http://uk.wikipedia.org/wiki/NVIDIA']]
    assert choose_concept(candidates, {'nvidia'}) == candidates[0]['uri']
    candidates.append(dict(uri='http://en.wikipedia.org/wiki/Other', type='org', label={'eng': 'Nvidia'}))
    assert choose_concept(candidates, {'nvidia'}) is None


def test_empty_200_without_archive_header_never_proves_historical_coverage():
    receipt = dict(parameters=PARAMS, data=[], usageHeaders={'req-archive': '0'})
    assert archive_assessment(receipt) == 'ARCHIVE_NOT_USED_ACCESS_UNCONFIRMED'
    receipt['usageHeaders']['req-archive'] = '1'
    assert archive_assessment(receipt) == 'ARCHIVE_USED_EMPTY_SAMPLE'
    receipt['data'] = [normalize_article(article(), KEY)]
    assert archive_assessment(receipt) == 'ARCHIVE_SAMPLE_RETURNED'
