"""Offline residual triage. This module cannot authorize or write a mapping.

The replay reason and source scope remain authoritative. A mixed-currency
partition is a review lead, never a currency conversion or an approval.
"""
from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib
import html
import re
from urllib.parse import urlsplit

UNITS = {name: 'USD' for name in (
    'REVENUE', 'NET_INCOME', 'OPERATING_INCOME', 'ASSETS', 'LIABILITIES')}
UNITS['DILUTED_EPS'] = 'USD/shares'
UNIT_REASON = 'UNREVIEWED_UNIT_TAXONOMY_OR_DIMENSIONS'


def diagnose(scope, as_of):
    if type(as_of) is not date:
        raise ValueError('EXPLICIT_AS_OF_DATE_REQUIRED')
    reason = scope['replayReason']
    rows = scope['rows']
    if not isinstance(rows, list):
        raise ValueError('SOURCE_ROWS_REQUIRED')
    flags = set()
    for row in rows:
        try:
            end = date.fromisoformat(row['period_end'])
            filed = date.fromisoformat(row['filing_date'])
            if filed < end:
                flags.add('FILING_BEFORE_PERIOD_END')
            if end > as_of or filed > as_of:
                flags.add('FUTURE_PERIOD_OR_FILING')
        except (KeyError, TypeError, ValueError):
            flags.add('MISSING_OR_INVALID_DATE')
    detail = reason
    if reason == 'RAW_OBSERVATION_MISMATCH':
        mismatches = {k.removeprefix('rawMismatch:')
                      for k, n in scope.get('diagnosticFlags', {}).items()
                      if k.startswith('rawMismatch:') and n > 0}
        detail = {
            frozenset({'form'}): 'FROZEN_RAW_FORM_MISMATCH_ONLY',
            frozenset({'filed'}): 'FROZEN_RAW_FILING_DATE_MISMATCH_ONLY',
            frozenset({'form', 'filed'}): 'FROZEN_RAW_FORM_AND_FILING_DATE_MISMATCH',
        }.get(frozenset(mismatches), 'FROZEN_RAW_OTHER_OR_UNCLASSIFIED_MISMATCH')
    if reason == UNIT_REASON:
        expected = UNITS.get(scope['metric'])
        if not rows:
            detail = 'NO_SOURCE_ROWS'
        elif any(not r.get('taxonomy') or not r.get('unit') for r in rows):
            detail = 'MISSING_SOURCE_METADATA'
        elif any(r['taxonomy'] != 'us-gaap' for r in rows):
            detail = 'UNREVIEWED_TAXONOMY'
        elif any(r.get('dimensions') not in (None, {}) for r in rows):
            detail = 'DIMENSIONED_SOURCE_PRESENT'
        elif expected is None:
            detail = 'UNSUPPORTED_METRIC'
        else:
            units = {r['unit'] for r in rows}
            detail = ('EXPECTED_UNIT_ABSENT' if expected not in units else
                      'EXPECTED_AND_OTHER_UNITS_MIXED' if units != {expected} else
                      'EXPECTED_UNIT_ONLY_REQUIRES_SEPARATE_REVIEW')
    return {'replayReason': reason, 'diagnosticReason': detail,
            'dateFlags': sorted(flags), 'mappingApproved': False,
            'sourceRowsChanged': False, 'currencyConverted': False}


def document_form_disagreement(row, raw, body, expected_sha):
    """Confirm a form-only disagreement; never normalize either source form.

    The actual cover can corroborate the stored form while companyfacts says
    something different. Neither observation is silently changed or approved.
    """
    if len(body) > 32 * 1024**2 or not re.fullmatch('[a-f0-9]{64}', expected_sha) or hashlib.sha256(body).hexdigest() != expected_sha:
        raise ValueError('METADATA_DOCUMENT_HASH_OR_BUDGET')
    u = urlsplit(row['source_url'])
    prefix = '/Archives/edgar/data/' + str(int(row['cik'])) + '/' + row['accession_number'].replace('-', '') + '/'
    if u.scheme != 'https' or u.netloc != 'www.sec.gov' or not u.path.startswith(prefix) or '..' in u.path or u.query or u.fragment:
        raise ValueError('METADATA_SOURCE_IDENTITY')
    expected = {'tag': row['source_tag'], 'unit': row['unit'],
                'start': row['period_start'], 'end': row['period_end'],
                'accession': row['accession_number'], 'filed': row['filing_date']}
    if any(raw.get(k) != v for k, v in expected.items()):
        raise ValueError('METADATA_NOT_FORM_ONLY')
    try:
        a, b = Decimal(str(row['value'])), Decimal(str(raw['value']))
        if not a.is_finite() or not b.is_finite() or a != b:
            raise ValueError('METADATA_VALUE_DISAGREEMENT')
    except (InvalidOperation, KeyError):
        raise ValueError('METADATA_VALUE_DISAGREEMENT') from None
    if {row['form_type'], raw.get('form')} != {'10-K', '10-K/A'}:
        raise ValueError('METADATA_ANNUAL_AMENDMENT_PAIR_REQUIRED')
    text = re.sub(r'<(script|style)\b[^>]*>.*?</\1>', ' ', body.decode('utf8', 'replace'), flags=re.I | re.S)
    text = re.sub(r'\s+', ' ', html.unescape(re.sub('<[^>]+>', ' ', text)))
    head = text[:3000]
    forms = re.findall(r'\bFORM\s+(10\s*-\s*K(?:\s*/\s*A)?)\b', head, re.I)
    cover = re.sub(r'\s+', '', forms[0]).upper() if forms else None
    if cover != row['form_type'] or (cover.endswith('/A') and not re.search(r'AMENDMENT\s+NO\.?\s*1', head, re.I)):
        raise ValueError('METADATA_COVER_FORM_NOT_CORROBORATED')
    normalized = text.casefold()
    note = re.search(r'explanatory note (.{100,2400})', normalized)
    exhibit_only = bool(note and all(phrase in note.group(1) for phrase in (
        'solely to furnish the interactive data files as exhibit 101',
        'inadvertently excluded from the original report',
        'no other changes are or have been made to the original report')))
    return {'diagnosticReason': 'DOCUMENT_COVER_CORROBORATES_STORED_FORM_RAW_DISAGREES',
            'storedForm': row['form_type'], 'rawForm': raw['form'],
            'documentCoverForm': cover, 'documentSha256': expected_sha,
            'amendmentPurpose': ('EXHIBIT101_ONLY_NO_OTHER_CHANGES_CONFIRMED'
                                 if exhibit_only else 'NOT_CLASSIFIED'),
            'mappingApproved': False, 'sourceMetadataChanged': False,
            'valueChanged': False}
