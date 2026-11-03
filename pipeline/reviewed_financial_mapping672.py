"""Explicitly reviewed historical mappings; never approve an arbitrary comparative."""
import hashlib
import json
import re
from datetime import date
from decimal import Decimal
from urllib.parse import urlsplit

POLICY = 'sec-reviewed-history672-v1'
REVENUE_TAGS = {'Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax'}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def reviewed_approval(scope, review, *, _reviewed_legacy_total=False):
    # Only the separately documented 699 statement-total contract may request
    # this identity check for a real legacy source tag. Ordinary callers retain
    # the exact modern-tag whitelist and proof bytes.
    allowed_tags = {'SalesRevenueNet'} if _reviewed_legacy_total else REVENUE_TAGS
    if _reviewed_legacy_total and review.get('basis') != 'UNCHANGED_TOTAL_CONFIRMED':
        raise ValueError('LEGACY_STATEMENT_IDENTITY_CONTRACT_REQUIRED')
    if review.get('metric') != 'REVENUE' or review.get('chosenTag') not in allowed_tags:
        raise ValueError('UNREVIEWED_METRIC_OR_TAG')
    if review.get('basis') not in {'UNCHANGED_TOTAL_RECLASSIFICATION', 'UNCHANGED_TOTAL_CONFIRMED', 'FULL_RETROSPECTIVE_REVENUE_STANDARD'}:
        raise ValueError('UNREVIEWED_ACCOUNTING_BASIS')
    if not scope or digest(scope) != review['sourceScopeHash']:
        raise ValueError('STORED_SOURCE_SCOPE_CHANGED')
    start, end = date.fromisoformat(review['start']), date.fromisoformat(review['end'])
    asof = date.fromisoformat(review['reviewedOn'])
    if not 335 <= (end-start).days <= 395 or end > asof:
        raise ValueError('INVALID_REVIEWED_ANNUAL_PERIOD')
    for row in scope:
        if (int(row['company_id']) != review['companyId'] or int(row['metric_id']) != review['metricId']
                or str(row['period_start']) != review['start'] or str(row['period_end']) != review['end']
                or row['metric_code'] != 'REVENUE' or row['taxonomy'] != 'us-gaap'
                or row['unit'] != 'USD' or row['dimensions'] not in (None, {})):
            raise ValueError('REVIEW_IDENTITY_OR_SCOPE_MISMATCH')
    selected = [r for r in scope if r['source_tag'] == review['chosenTag'] and r['form_type'] in {'10-K','10-K/A'}]
    if not selected:
        raise ValueError('REVIEWED_SOURCE_NOT_FOUND')
    latest = max(str(r['filing_date']) for r in selected)
    selected = [r for r in selected if str(r['filing_date']) == latest]
    expected = Decimal(review['value'])
    if not expected.is_finite() or {Decimal(str(r['value'])) for r in selected} != {expected}:
        raise ValueError('REVIEWED_VALUE_CONFLICT')
    if latest != review['filingDate'] or date.fromisoformat(latest) > asof:
        raise ValueError('REVIEWED_FILING_CHANGED')
    if {r['accession_number'] for r in selected} != set(review['accessions']):
        raise ValueError('REVIEWED_ACCESSION_CHANGED')
    if {r['source_fact_hash'] for r in selected} != set(review['sourceFactHashes']):
        raise ValueError('REVIEWED_FACT_HASH_CHANGED')
    for row in selected:
        cik = str(int(row['cik']))
        url = urlsplit(row['source_url'])
        expected_path = '/Archives/edgar/data/' + cik + '/' + row['accession_number'].replace('-', '') + '/'
        if not re.fullmatch(r'[a-f0-9]{64}', row['source_fact_hash']) or url.scheme != 'https' or url.netloc != 'www.sec.gov' or not url.path.startswith(expected_path) or '..' in url.path or url.query or url.fragment:
            raise ValueError('REVIEWED_SOURCE_PROVENANCE_INVALID')
    originals = [r for r in scope if r['source_tag'] == 'SalesRevenueNet' and str(r['report_date']) == review['end'] and r['form_type'] in {'10-K','10-K/A'}]
    if not originals:
        raise ValueError('ORIGINAL_ANNUAL_REPORT_REQUIRED')
    newest_original = max(str(r['filing_date']) for r in originals)
    originals = [r for r in originals if str(r['filing_date']) == newest_original]
    original = Decimal(review['originalValue'])
    if {Decimal(str(r['value'])) for r in originals} != {original}:
        raise ValueError('ORIGINAL_VALUE_CONFLICT')
    if review['basis'] in {'UNCHANGED_TOTAL_RECLASSIFICATION', 'UNCHANGED_TOTAL_CONFIRMED'} and original != expected:
        raise ValueError('UNCHANGED_TOTAL_DOES_NOT_MATCH')
    if review['basis'] == 'FULL_RETROSPECTIVE_REVENUE_STANDARD' and expected-original != Decimal(review['revenueStandardAdjustment']):
        raise ValueError('RESTATEMENT_RECONCILIATION_MISMATCH')
    if not review.get('officialDocuments') or not review.get('reviewExplanation'):
        raise ValueError('HUMAN_DOCUMENT_REVIEW_REQUIRED')
    proof = {'policy': POLICY, 'review': review, 'sources': selected,
             'originalSources': originals, 'hashMeaning': 'stored fact hashes, not downloaded document byte hashes'}
    return {'company_id': review['companyId'], 'metric_id': review['metricId'],
            'source_tag': review['chosenTag'], 'end': review['end'],
            'proof': proof, 'proof_hash': digest(proof)}
