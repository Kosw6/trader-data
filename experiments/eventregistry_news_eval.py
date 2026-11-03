"""At most ten requests, three samples each; no AWS, DB, model or crawling."""
import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'trader-data'))
from collectors.eventregistry_client import (NewsApiClient, NewsApiError, TOKEN_ENV,
                                            SUGGEST, ARTICLES, article_params, assess, archive_assessment)

COMPANIES = [('NVDA', 'Nvidia', {'nvidia', 'nvidia corporation'}),
             ('AAPL', 'Apple Inc.', {'apple inc.', 'apple inc', 'apple'}),
             ('TSLA', 'Tesla, Inc.', {'tesla, inc.', 'tesla inc', 'tesla, inc', 'tesla'})]


def save(path, data):
    with path.open('x', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def choose_concept(candidates, labels):
    matches = [c for c in candidates if c.get('type') == 'org' and isinstance(c.get('label'), dict)
               and c['label'].get('eng', '').casefold() in labels and c.get('uri')]
    # The provider suggests localized Wikipedia URIs for the same company.
    # English article queries use the unique English organization concept.
    english = [c for c in matches if urlsplit(c['uri']).hostname == 'en.wikipedia.org']
    if len(english) == 1:
        return english[0]['uri']
    return matches[0]['uri'] if len(matches) == 1 else None


def run(out, token, now):
    out.mkdir(parents=True, exist_ok=False)
    save(out / 'plan.json', dict(provider='NEWSAPI_AI_EVENTREGISTRY', maxRequests=10,
         conservativeLocalReservation=30, samplesPerQuery=3, pagesPerQuery=1,
         archiveYears=[2016], archiveQueriesMaximum=2, recentDays=7,
         paidUpgrade=False, tokenInJsonBodyOnly=True, automaticRetries=0,
         fullBodyRetained=False, rawResponseRetained=False,
         independentOriginalArticleTruthVerified=False))
    client = NewsApiClient(token)
    cases, articles, concepts = [], {}, {}
    held = None
    archive_rejected = False

    def request(case, endpoint, params):
        nonlocal archive_rejected
        # Durable intent avoids concealing an attempted request if transport fails.
        save(out / (case + '-intent.json'), dict(endpoint=endpoint, parameters=params))
        try:
            r = client.post(endpoint, params)
            save(out / (case + '.json'), r)
            checked = assess(r['data'], params) if endpoint == ARTICLES else None
            cases.append(dict(case=case, status='RETURNED_SAMPLE' if r['data'] else 'EMPTY_UNCONFIRMED_COVERAGE',
                              http=r['http'], seconds=r['seconds'], meta=r.get('meta'),
                              usageHeaders=r['usageHeaders'], assessment=checked,
                              archiveAssessment=archive_assessment(r) if endpoint == ARTICLES else None))
            if endpoint == ARTICLES:
                for a in r['data']:
                    existing = articles.get(a['uri'])
                    if existing and (existing['title'], existing['url'], existing['date']) != (a['title'], a['url'], a['date']):
                        raise NewsApiError('DUPLICATE_URI_CONTENT_CONFLICT')
                    if existing:
                        existing['observedCases'].append(case)
                    else:
                        articles[a['uri']] = {**a, 'observedCases': [case]}
                if checked['dateViolations'] or checked['conceptAssociationMissing']:
                    raise NewsApiError('SAMPLE_DATE_OR_CONCEPT_REVIEW_REQUIRED')
            return r['data']
        except NewsApiError as e:
            row = dict(case=case, status='ARCHIVE_ACCESS_REJECTED' if e.archive_denied else 'HELD',
                       code=e.code, http=e.status, stop=e.stop, receipt=e.receipt)
            save(out / (case + '-error.json'), row)
            cases.append({k: v for k, v in row.items() if k != 'receipt'})
            if e.archive_denied:
                archive_rejected = True
                return []
            raise

    try:
        start, end = (now - timedelta(days=7)).date().isoformat(), now.date().isoformat()
        for symbol, name, labels in COMPANIES:
            data = request('concept_' + symbol, SUGGEST,
                           dict(prefix=name, source=['org'], lang='eng', conceptLang='eng', page=1, count=5))
            uri = choose_concept(data, labels)
            concepts[symbol] = dict(name=name, conceptUri=uri, uniqueOrganizationMatch=bool(uri),
                                    exchangeTickerIdentityIndependentlyVerified=False)
            if not uri:
                cases.append(dict(case='recent_' + symbol, status='SKIPPED_IDENTITY_AMBIGUOUS'))
                continue
            request('recent_' + symbol, ARTICLES, article_params(start, end, concept=uri))
        for case, keyword in [('macro_rates', 'Federal Reserve'), ('macro_geopolitics', 'sanctions')]:
            request(case, ARTICLES, article_params(start, end, keyword=keyword))
        uri = concepts.get('NVDA', {}).get('conceptUri')
        if uri:
            request('history_NVDA_2016', ARTICLES,
                    article_params('2016-01-01', '2016-12-31', concept=uri, historical=True))
        if not archive_rejected:
            request('history_rates_2016', ARTICLES,
                    article_params('2016-01-01', '2016-12-31', keyword='Federal Reserve', historical=True))
        else:
            cases.append(dict(case='history_rates_2016', status='SKIPPED_AFTER_ARCHIVE_ACCESS_REJECTION'))
    except NewsApiError as e:
        held = e.code
    finally:
        client.close()
        save(out / 'concept-identities.json', concepts)
        save(out / 'articles.json', list(articles.values()))
        save(out / 'case-results.json', cases)
        history = [c for c in cases if c['case'].startswith('history_') and c.get('assessment', {}).get('returned', 0)]
        summary = dict(status='HELD' if held else 'BOUNDED_EVALUATION_FINISHED_WITH_LIMITATIONS',
                       stopReason=held, requestsAttempted=client.attempts,
                       localReservationUsed=client.reserved, actualReportedTokens=client.reported_tokens,
                       allTokenHeadersKnown=client.tokens_known, uniqueArticles=len(articles),
                       history2016SampleReturned=bool(history), archiveAccessRejected=archive_rejected,
                       wholeArchiveCoverageVerified=False, originalArticleTruthVerified=False,
                       publicRedistributionApproved=False, paidPlanActivated=False,
                       awsChanges=0, dbWrites=0, modelCalls=0, sourcePagesFetched=0)
        save(out / 'summary.json', summary)
        print(json.dumps(summary))
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()
    token = dotenv_values(ROOT / 'trader-data/.env.news-api', encoding='utf-8-sig').get(TOKEN_ENV, '')
    if not args.run:
        print(json.dumps(dict(keyConfigured=bool(token), maxRequests=10, networkRequests=0)))
    else:
        if not token:
            raise SystemExit('NEWSAPI_AI_KEY_NOT_CONFIGURED')
        now = datetime.now(timezone.utc)
        run(ROOT / 'output' / ('news-eventregistry-eval-' + now.strftime('%Y%m%dT%H%M%SZ')), token, now)
