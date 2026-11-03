"""Finite NewsAPI.ai reads; no SDK retries, full article retention or URL keys."""
import hashlib
import json
import re
import time
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

import requests
from requests.adapters import HTTPAdapter

from collectors.marketaux_client import safe_url, sanitized

TOKEN_ENV = 'TRADER_NEWS_EVENTREGISTRY_API_KEY'
BASE = 'https://eventregistry.org'
SUGGEST = '/api/v1/suggestConceptsFast'
ARTICLES = '/api/v1/article/getArticles'
HEADERS = {'req-tokens', 'req-action', 'req-archive', 'x-ratelimit-limit',
           'x-ratelimit-remaining', 'x-ratelimit-used', 'x-ratelimit-request-limit',
           'x-ratelimit-request-remaining'}


class NewsApiError(RuntimeError):
    def __init__(self, code, status=None, *, stop=True, archive_denied=False, receipt=None):
        self.code, self.status, self.stop = code, status, stop
        self.archive_denied, self.receipt = archive_denied, receipt
        super().__init__(code)


def article_params(start, end, *, concept=None, keyword=None, historical=False):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last or (historical and first.year != last.year):
        raise ValueError('SINGLE_YEAR_ARCHIVE_ONLY')
    if bool(concept) == bool(keyword):
        raise ValueError('EXACTLY_ONE_QUERY_TARGET_REQUIRED')
    p = dict(action='getArticles', resultType='articles', dateStart=start, dateEnd=end,
             lang='eng', dataType='news', articlesPage=1, articlesCount=3,
             articlesSortBy='date', articlesSortByAsc=False,
             isDuplicateFilter='skipDuplicates', includeArticleConcepts=True,
             articleBodyLen=400, includeArticleImage=False)
    p.update({'conceptUri': concept} if concept else {'keyword': keyword})
    if not historical:
        p['forceMaxDataTimeWindow'] = 31
    return p


def normalize_article(a, secret):
    if not isinstance(a, dict) or not a.get('uri') or not isinstance(a.get('title'), str):
        raise NewsApiError('ARTICLE_ID_TITLE_INVALID')
    url = safe_url(a.get('url'))
    try:
        date.fromisoformat(a.get('date', ''))
    except (TypeError, ValueError):
        raise NewsApiError('ARTICLE_DATE_INVALID') from None
    if not url:
        raise NewsApiError('ARTICLE_URL_INVALID')
    concepts = a.get('concepts', [])
    if not isinstance(concepts, list):
        raise NewsApiError('ARTICLE_CONCEPTS_INVALID')
    source = a.get('source', {})
    if not isinstance(source, dict):
        raise NewsApiError('ARTICLE_SOURCE_INVALID')
    item = {k: a.get(k) for k in ['uri', 'title', 'date', 'time', 'dateTime',
                                 'dateTimePub', 'lang', 'isDuplicate', 'eventUri']}
    item.update(url=url, excerpt=str(a.get('body') or '')[:400],
                source={k: source.get(k) for k in ['uri', 'title', 'dataType']},
                concepts=[{k: c.get(k) for k in ['uri', 'type', 'label', 'score']}
                          for c in concepts if isinstance(c, dict)],
                provider='NEWSAPI_AI_EVENTREGISTRY',
                content_kind='PROVIDER_METADATA_AND_SHORT_EXCERPT',
                event_date=None, causal_stock_impact_verified=False)
    return sanitized(item, secret)


def assess(articles, params):
    uris = [a['uri'] for a in articles]
    target = params.get('conceptUri')
    keyword = params.get('keyword', '')
    labels = [c.get('label', {}).get('eng', '') for a in articles for c in a['concepts']
              if c.get('uri') == target and isinstance(c.get('label'), dict)]
    return dict(returned=len(articles),
                dateViolations=[a['uri'] for a in articles
                                if not params['dateStart'] <= a['date'] <= params['dateEnd']],
                duplicateIds=len(uris) - len(set(uris)),
                conceptAssociationMissing=[a['uri'] for a in articles if target and
                                          not any(c.get('uri') == target for c in a['concepts'])],
                titleMentionCount=sum(any(s and s.casefold() in a['title'].casefold()
                                          for s in ([keyword] if keyword else labels)) for a in articles),
                associationIsNotDirectRelevance=True, archiveCompletenessVerified=False,
                originalArticleTruthVerified=False)


def archive_assessment(receipt):
    """A 200/empty response is not proof the account searched historical data."""
    if 'forceMaxDataTimeWindow' in receipt['parameters']:
        return 'RECENT_QUERY'
    if receipt.get('usageHeaders', {}).get('req-archive') != '1':
        return 'ARCHIVE_NOT_USED_ACCESS_UNCONFIRMED'
    return 'ARCHIVE_SAMPLE_RETURNED' if receipt.get('data') else 'ARCHIVE_USED_EMPTY_SAMPLE'


class NewsApiClient:
    def __init__(self, token, *, max_requests=10, reservation_cap=30, session=None,
                 min_interval=1.05):
        if not isinstance(token, str) or not re.fullmatch(r'[A-Za-z0-9_.\-]{12,512}', token):
            raise NewsApiError('NEWSAPI_AI_KEY_MISSING_OR_INVALID')
        if type(max_requests) is not int or not 1 <= max_requests <= 10:
            raise ValueError('REQUEST_CAP_INVALID')
        if type(reservation_cap) is not int or not 1 <= reservation_cap <= 30:
            raise ValueError('RESERVATION_CAP_INVALID')
        self.__token = token
        self.attempts = self.reserved = self.reported_tokens = 0
        self.tokens_known = True
        self.max_requests, self.reservation_cap = max_requests, reservation_cap
        self.last, self.min_interval = 0., min_interval
        self.session = session or requests.Session()
        self.session.trust_env = False
        self.session.mount('https://', HTTPAdapter(max_retries=0))
        self.session.headers.update({'Accept': 'application/json',
                                     'User-Agent': 'RevestLab-News-Evaluation/1.0'})

    def __repr__(self):
        return f'NewsApiClient(key=[REDACTED], attempts={self.attempts})'

    def close(self):
        self.session.close()

    def post(self, endpoint, params):
        if endpoint not in {SUGGEST, ARTICLES}:
            raise ValueError('ENDPOINT_NOT_ALLOWED')
        if any(k.casefold() in {'apikey', 'api_key', 'token', 'authorization'} for k in params):
            raise ValueError('CALLER_CREDENTIAL_OVERRIDE')
        allowed = ({'prefix', 'source', 'lang', 'conceptLang', 'page', 'count'}
                   if endpoint == SUGGEST else
                   set(article_params('2016-01-01', '2016-12-31', keyword='test')) | {'conceptUri'})
        if set(params) - allowed:
            raise ValueError('PARAMETER_NOT_ALLOWED')
        if endpoint == SUGGEST:
            if params.get('count') != 5 or params.get('page') != 1:
                raise ValueError('SUGGEST_CAP_REQUIRED')
            reserve = 5
        else:
            if params.get('articlesCount') != 3 or params.get('articlesPage') != 1:
                raise ValueError('ARTICLE_CAP_REQUIRED')
            if params.get('articleBodyLen') != 400:
                raise ValueError('EXCERPT_CAP_REQUIRED')
            if params.get('action') != 'getArticles' or params.get('resultType') != 'articles':
                raise ValueError('ARTICLE_ACTION_REQUIRED')
            start, end = date.fromisoformat(params['dateStart']), date.fromisoformat(params['dateEnd'])
            if start > end:
                raise ValueError('DATE_RANGE_INVALID')
            if 'forceMaxDataTimeWindow' not in params:
                if start.year != end.year:
                    raise ValueError('SINGLE_YEAR_ARCHIVE_ONLY')
                reserve = 5
            else:
                if params['forceMaxDataTimeWindow'] != 31:
                    raise ValueError('RECENT_WINDOW_REQUIRED')
                reserve = 1
        if self.attempts >= self.max_requests or self.reserved + reserve > self.reservation_cap:
            raise NewsApiError('LOCAL_EXPERIMENT_CAP_REACHED')
        if self.reported_tokens > self.reservation_cap:
            raise NewsApiError('REPORTED_TOKEN_CAP_REACHED')
        time.sleep(max(0., self.min_interval - (time.monotonic() - self.last)))
        self.last = time.monotonic()
        self.attempts += 1
        self.reserved += reserve
        started = time.monotonic()
        try:
            with self.session.post(BASE + endpoint, json={**params, 'apiKey': self.__token},
                                   timeout=(5, 30), allow_redirects=False, stream=True) as r:
                raw = bytearray()
                for block in r.iter_content(65536):
                    raw.extend(block)
                    if len(raw) > 2 * 1024 ** 2:
                        raise NewsApiError('NEWS_RESPONSE_SIZE_LIMIT')
                headers = {k.lower(): v for k, v in r.headers.items() if k.lower() in HEADERS}
                try:
                    used = Decimal(headers['req-tokens'])
                    if not used.is_finite() or used < 0:
                        raise ValueError()
                    self.reported_tokens += float(used)
                except (KeyError, TypeError, ValueError, InvalidOperation):
                    self.tokens_known = False
                receipt = dict(provider='NEWSAPI_AI_EVENTREGISTRY', endpoint=endpoint,
                               parameters=params, http=r.status_code, attempt=self.attempts,
                               seconds=round(time.monotonic() - started, 3),
                               received_at=datetime.now(timezone.utc).isoformat(),
                               response_sha256=hashlib.sha256(raw).hexdigest(),
                               response_bytes=len(raw), usageHeaders=sanitized(headers, self.__token))
                try:
                    payload = json.loads(raw)
                except (ValueError, UnicodeError):
                    payload = None
                if r.status_code != 200 or isinstance(payload, dict) and payload.get('error'):
                    # Inspect errors in memory; persist neither raw messages nor request objects.
                    msg = str(payload.get('error', '')) if isinstance(payload, dict) else ''
                    archive_denied = ('forceMaxDataTimeWindow' not in params and endpoint == ARTICLES
                                      and r.status_code in {200, 400, 401, 403}
                                      and bool(re.search(r'archive|historical|past 30|past 31|last 30|last 31', msg, re.I))
                                      and bool(re.search(r'access|allow|paid|paying|permission|plan|subscri|limit|upgrade', msg, re.I)))
                    code = 'ARCHIVE_ACCESS_REJECTED' if archive_denied else {
                        400: 'PARAMETERS_REJECTED', 401: 'AUTH_OR_QUOTA_REJECTED',
                        403: 'ACCOUNT_OR_QUOTA_REJECTED', 429: 'RATE_LIMIT',
                    }.get(r.status_code, 'PROVIDER_REQUEST_REJECTED')
                    raise NewsApiError(code, r.status_code, archive_denied=archive_denied,
                                       stop=not archive_denied, receipt=receipt)
                if endpoint == SUGGEST:
                    if not isinstance(payload, list) or len(payload) > 5:
                        raise NewsApiError('CONCEPT_RESPONSE_SCHEMA_INVALID', receipt=receipt)
                    data = [{k: c.get(k) for k in ['uri', 'type', 'label']} for c in payload if isinstance(c, dict)]
                    receipt.update(data=sanitized(data, self.__token))
                else:
                    block = payload.get('articles') if isinstance(payload, dict) else None
                    if not isinstance(block, dict) or not isinstance(block.get('results'), list) or len(block['results']) > 3:
                        raise NewsApiError('ARTICLE_RESPONSE_SCHEMA_INVALID', receipt=receipt)
                    receipt.update(data=[normalize_article(a, self.__token) for a in block['results']],
                                   meta={k: block.get(k) for k in ['page', 'count', 'pages', 'totalResults']})
                return receipt
        except requests.RequestException:
            raise NewsApiError('NEWSAPI_TRANSPORT_ERROR') from None
