"""Use only the three unused calls of the original ten-call experiment."""
import json
import sys
from decimal import Decimal
from pathlib import Path
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'trader-data'))
from collectors.eventregistry_client import (NewsApiClient, NewsApiError, TOKEN_ENV,
                                            ARTICLES, article_params, assess, archive_assessment)
from experiments.eventregistry_news_eval import COMPANIES, choose_concept, save


def main():
    parent = ROOT / 'output/news-eventregistry-eval-20261101T115518Z'
    old = json.loads((parent / 'summary.json').read_text(encoding='utf-8'))
    assert old['requestsAttempted'] == 7 and old['localReservationUsed'] == 23
    out = parent / 'remaining-three-calls'
    out.mkdir(exist_ok=False)
    save(out / 'plan.json', dict(inheritedRequests=7, additionalRequestsCap=3,
         cumulativeRequestsCap=10, inheritedReservation=23, additionalReservation=7,
         reason='Resolve localized concepts from saved receipts; no repeated suggestions'))
    token = dotenv_values(ROOT / 'trader-data/.env.news-api', encoding='utf-8-sig').get(TOKEN_ENV, '')
    c = NewsApiClient(token, max_requests=3, reservation_cap=7)
    old_recent = json.loads((parent / 'recent_TSLA.json').read_text(encoding='utf-8'))['parameters']
    identities, cases, stop = {}, [], None
    queries = []
    for symbol, _, labels in COMPANIES[:2]:
        candidates = json.loads((parent / ('concept_' + symbol + '.json')).read_text(encoding='utf-8'))['data']
        uri = choose_concept(candidates, labels)
        assert uri, 'EXACT_ENGLISH_ORGANIZATION_REQUIRED'
        identities[symbol] = dict(conceptUri=uri, method='Unique English organization from saved provider suggestions',
                                  exchangeTickerIdentityIndependentlyVerified=False)
        queries.append(('recent_' + symbol, article_params(old_recent['dateStart'], old_recent['dateEnd'], concept=uri)))
    queries.append(('history_NVDA_2016', article_params('2016-01-01', '2016-12-31', concept=identities['NVDA']['conceptUri'], historical=True)))
    try:
        for case, params in queries:
            save(out / (case + '-intent.json'), dict(endpoint=ARTICLES, parameters=params))
            try:
                r = c.post(ARTICLES, params)
                save(out / (case + '.json'), r)
                checked = assess(r['data'], params)
                cases.append(dict(case=case, http=r['http'], meta=r['meta'], assessment=checked,
                                  archiveAssessment=archive_assessment(r), usageHeaders=r['usageHeaders']))
                if checked['dateViolations'] or checked['conceptAssociationMissing']:
                    raise NewsApiError('SAMPLE_DATE_OR_CONCEPT_REVIEW_REQUIRED')
            except NewsApiError as e:
                save(out / (case + '-error.json'), dict(code=e.code, http=e.status, receipt=e.receipt))
                cases.append(dict(case=case, code=e.code, http=e.status))
                if e.stop: raise
    except NewsApiError as e:
        stop = e.code
    finally:
        c.close()
        save(out / 'identities.json', identities)
        save(out / 'case-results.json', cases)
        all_receipts = []
        for folder in [parent, out]:
            for f in folder.glob('*.json'):
                data = json.loads(f.read_text(encoding='utf-8'))
                if isinstance(data, dict) and 'response_sha256' in data:
                    all_receipts.append((f, data))
        costs = [Decimal(r['usageHeaders']['req-tokens']) for _, r in all_receipts]
        unique, observations, conflicts = {}, 0, []
        for f, r in all_receipts:
            if r['endpoint'] != ARTICLES: continue
            for a in r['data']:
                observations += 1
                if a['uri'] in unique:
                    previous = unique[a['uri']]
                    if any(previous[k] != a[k] for k in ['title', 'date', 'url']): conflicts.append(a['uri'])
                    previous['observedCases'].append(f.stem)
                else: unique[a['uri']] = {**a, 'observedCases': [f.stem]}
        save(out / 'articles-combined.json', list(unique.values()))
        report = dict(status='HELD' if stop or conflicts else 'EVALUATED_WITH_ARCHIVE_ACCESS_UNCONFIRMED',
                      stopReason=stop, requestsAttemptedCumulative=old['requestsAttempted'] + c.attempts,
                      reservationCumulative=old['localReservationUsed'] + c.reserved,
                      receiptCount=len(all_receipts), reportedTokensCumulative=float(sum(costs)),
                      uniqueArticles=len(unique), articleAppearances=observations,
                      duplicateAppearances=observations-len(unique), contentConflicts=conflicts,
                      historicalReceipts=[dict(case=f.stem, assessment=archive_assessment(r), returned=len(r['data']))
                                          for f,r in all_receipts if f.stem.startswith('history_')],
                      originalArticleTruthVerified=False, archiveCompletenessVerified=False,
                      commercialUseApproved=False, paidUpgrade=False, awsChanges=0, dbWrites=0, modelCalls=0)
        save(out / 'completion.json', report)
        print(json.dumps(report))


if __name__ == '__main__': main()
