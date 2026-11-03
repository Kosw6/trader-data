from datetime import date, datetime
import copy
import pytest
from pipeline.residual_diagnostics705 import diagnose, UNIT_REASON

AS_OF = date(2026, 10, 31)


def scope(units=('USD',), **row):
    return {'metric': 'REVENUE', 'replayReason': UNIT_REASON,
            'rows': [dict(taxonomy='us-gaap', unit=u, dimensions=None,
                          period_end='2024-12-31', filing_date='2025-02-01', **row)
                     for u in units]}


@pytest.mark.parametrize('units,expected', [
    (('USD', 'EUR'), 'EXPECTED_AND_OTHER_UNITS_MIXED'),
    (('EUR',), 'EXPECTED_UNIT_ABSENT'),
    (('USD',), 'EXPECTED_UNIT_ONLY_REQUIRES_SEPARATE_REVIEW'),
    (('',), 'MISSING_SOURCE_METADATA'),
    ((), 'NO_SOURCE_ROWS'),
])
def test_currency_diagnostic_never_approves_or_mutates(units, expected):
    s = scope(units); before = copy.deepcopy(s)
    d = diagnose(s, AS_OF)
    assert d['diagnosticReason'] == expected
    assert not d['mappingApproved'] and not d['currencyConverted']
    assert s == before and not d['sourceRowsChanged']


def test_eps_uses_per_share_unit():
    s = scope(('USD/shares', 'USD')); s['metric'] = 'DILUTED_EPS'
    assert diagnose(s, AS_OF)['diagnosticReason'] == 'EXPECTED_AND_OTHER_UNITS_MIXED'


@pytest.mark.parametrize('key,value,expected', [
    ('taxonomy', 'ifrs-full', 'UNREVIEWED_TAXONOMY'),
    ('dimensions', {'segment': 'A'}, 'DIMENSIONED_SOURCE_PRESENT'),
    ('unit', None, 'MISSING_SOURCE_METADATA'),
])
def test_unsafe_scope_dominates_unit_partition(key, value, expected):
    s = scope(('USD', 'EUR')); s['rows'][0][key] = value
    assert diagnose(s, AS_OF)['diagnosticReason'] == expected


def test_unknown_metric_is_not_inferred():
    s = scope(); s['metric'] = 'UNKNOWN'
    assert diagnose(s, AS_OF)['diagnosticReason'] == 'UNSUPPORTED_METRIC'


def test_invalid_dates_remain_review_not_approved():
    s = scope(); s['replayReason'] = 'INVALID_VALUE_OR_DATE'
    s['rows'][0].update(period_end='2027-01-01', filing_date='2026-01-01')
    d = diagnose(s, AS_OF)
    assert d['dateFlags'] == ['FILING_BEFORE_PERIOD_END', 'FUTURE_PERIOD_OR_FILING']
    assert d['replayReason'] == d['diagnosticReason'] == 'INVALID_VALUE_OR_DATE'


def test_bad_date_flag_is_explicit():
    s = scope(); s['rows'][0]['filing_date'] = 'broken'
    assert diagnose(s, AS_OF)['dateFlags'] == ['MISSING_OR_INVALID_DATE']


@pytest.mark.parametrize('as_of', [None, '2026-10-31', datetime(2026, 10, 31)])
def test_requires_explicit_date(as_of):
    with pytest.raises(ValueError, match='EXPLICIT_AS_OF_DATE_REQUIRED'):
        diagnose(scope(), as_of)


@pytest.mark.parametrize('flags,expected', [
    ({'rawMismatch:form': 3}, 'FROZEN_RAW_FORM_MISMATCH_ONLY'),
    ({'rawMismatch:filed': 2}, 'FROZEN_RAW_FILING_DATE_MISMATCH_ONLY'),
    ({'rawMismatch:form': 3, 'rawMismatch:filed': 2}, 'FROZEN_RAW_FORM_AND_FILING_DATE_MISMATCH'),
    ({'rawMismatch:value': 1}, 'FROZEN_RAW_OTHER_OR_UNCLASSIFIED_MISMATCH'),
])
def test_frozen_metadata_disagreement_is_not_a_value_correction(flags, expected):
    s = scope(); s.update(replayReason='RAW_OBSERVATION_MISMATCH', diagnosticFlags=flags)
    d = diagnose(s, AS_OF)
    assert d['diagnosticReason'] == expected and not d['mappingApproved']


def test_report_distinguishes_scope_counts_from_observation_counts(tmp_path):
    import gzip, json
    from workers.residual_diagnostics705 import report
    s = scope(); s.update(companyId=1, metricId=22, end='2024-12-31',
                          diagnosticFlags={'rawMismatch:form': 7})
    ledger = tmp_path / 'ledger.gz'; resolved = tmp_path / 'resolved.json'
    with gzip.open(ledger, 'wt', encoding='utf8') as f:
        f.write(json.dumps(s) + '\n')
    resolved.write_text('[]', encoding='utf8')
    r = report(ledger, resolved, AS_OF)
    assert r['frozenDiagnosticScopeCountsMayOverlap']['rawMismatch:form'] == 1
    assert r['frozenDiagnosticObservationCountsMayOverlap']['rawMismatch:form'] == 7
    assert r['remainingScopes'] == 1 and r['databaseWrites'] == r['mappingApprovals'] == 0


@pytest.mark.parametrize('failure', ['duplicate_source', 'duplicate_resolved', 'foreign_resolved', 'limit'])
def test_report_rejects_incomplete_key_reconciliation(tmp_path, failure):
    import gzip, json
    from workers.residual_diagnostics705 import report
    s = scope(); s.update(companyId=1, metricId=22, end='2024-12-31')
    ledger = tmp_path / 'ledger.gz'; resolved = tmp_path / 'resolved.json'
    with gzip.open(ledger, 'wt', encoding='utf8') as f:
        f.write(json.dumps(s) + '\n')
        if failure == 'duplicate_source': f.write(json.dumps(s) + '\n')
    k = {name: s[name] for name in ('companyId', 'metricId', 'end')}
    resolved.write_text(json.dumps([k, k] if failure == 'duplicate_resolved' else
                                   [dict(k, companyId=2)] if failure == 'foreign_resolved' else []), encoding='utf8')
    with pytest.raises(ValueError):
        report(ledger, resolved, AS_OF, maximum=0 if failure == 'limit' else 200000)


def form_case():
    import hashlib
    row = dict(cik='0000778164', source_url='https://www.sec.gov/Archives/edgar/data/778164/000101968716005485/report.htm',
               accession_number='0001019687-16-005485', source_tag='Revenues', unit='USD',
               period_start='2013-01-01', period_end='2013-12-31',
               filing_date='2016-03-16', value='123.00', form_type='10-K/A')
    raw = dict(tag='Revenues', unit='USD', start='2013-01-01', end='2013-12-31',
               accession='0001019687-16-005485', filed='2016-03-16', value='123', form='10-K')
    body = b'<html>FORM 10-K/A AMENDMENT NO. 1 ANNUAL REPORT</html>'
    return row, raw, body, hashlib.sha256(body).hexdigest()


def test_cover_diagnostic_preserves_both_conflicting_forms():
    from pipeline.residual_diagnostics705 import document_form_disagreement
    row, raw, body, sha = form_case(); before = copy.deepcopy((row, raw))
    r = document_form_disagreement(row, raw, body, sha)
    assert (r['storedForm'], r['rawForm'], r['documentCoverForm']) == ('10-K/A', '10-K', '10-K/A')
    assert (row, raw) == before and not r['mappingApproved'] and not r['sourceMetadataChanged']


@pytest.mark.parametrize('field,value', [('end', '2014-12-31'), ('filed', '2016-03-17'),
    ('accession', 'wrong'), ('unit', 'EUR'), ('tag', 'SalesRevenueNet'),
    ('value', '124'), ('value', 'NaN'), ('form', '20-F')])
def test_cover_does_not_override_other_raw_disagreements(field, value):
    from pipeline.residual_diagnostics705 import document_form_disagreement
    row, raw, body, sha = form_case(); raw[field] = value
    with pytest.raises(ValueError): document_form_disagreement(row, raw, body, sha)


@pytest.mark.parametrize('change', ['bytes', 'host', 'accession_path', 'cover', 'amendment', 'script_only'])
def test_cover_requires_bound_document_and_actual_first_form(change):
    import hashlib
    from pipeline.residual_diagnostics705 import document_form_disagreement
    row, raw, body, sha = form_case()
    if change == 'bytes': body += b'changed'
    elif change == 'host': row['source_url'] = row['source_url'].replace('www.sec.gov', 'other.example')
    elif change == 'accession_path': row['source_url'] = row['source_url'].replace('000101968716005485', '000101968716005486')
    else:
        body = {'cover': b'FORM 10-K ANNUAL REPORT',
                'amendment': b'FORM 10-K/A ANNUAL REPORT',
                'script_only': b'<script>FORM 10-K/A AMENDMENT NO. 1</script>ANNUAL REPORT'}[change]
        sha = hashlib.sha256(body).hexdigest()
    with pytest.raises(ValueError): document_form_disagreement(row, raw, body, sha)


@pytest.mark.parametrize('change,expected', [('none', True), ('no_note', False),
    ('no_nochange_statement', False), ('script_only', False)])
def test_exhibit_only_note_does_not_approve_or_infer_a_restated_amount(change, expected):
    import hashlib
    from pipeline.residual_diagnostics705 import document_form_disagreement
    row, raw, body, sha = form_case()
    note = (b' EXPLANATORY NOTE The purpose of this Amendment No. 1 is solely to furnish '
            b'the Interactive Data files as Exhibit 101, which were inadvertently excluded '
            b'from the Original Report. No other changes are or have been made to the Original Report.')
    if change == 'no_note': note = b'Financial restatement of net income.'
    if change == 'no_nochange_statement': note = note.split(b'No other')[0]
    if change == 'script_only': note = b'<script>' + note + b'</script>'
    body += note; sha = hashlib.sha256(body).hexdigest()
    result = document_form_disagreement(row, raw, body, sha)
    assert (result['amendmentPurpose'] == 'EXHIBIT101_ONLY_NO_OTHER_CHANGES_CONFIRMED') == expected
    assert not result['mappingApproved'] and not result['valueChanged']
