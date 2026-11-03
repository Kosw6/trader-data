from copy import deepcopy
import pytest
from pipeline.reviewed_financial_mapping672 import digest, reviewed_approval


def fixture():
    row = dict(company_id=1, metric_id=2, metric_code='REVENUE', taxonomy='us-gaap', unit='USD',
        dimensions=None, cik='0000000001', period_start='2022-07-01', period_end='2023-06-30',
        form_type='10-K', source_tag='SalesRevenueNet', value='100', report_date='2023-06-30',
        filing_date='2023-08-01', accession_number='0000000001-23-000001', source_fact_hash='a'*64,
        source_url='https://www.sec.gov/Archives/edgar/data/1/000000000123000001/original.htm')
    restated = dict(row, source_tag='RevenueFromContractWithCustomerExcludingAssessedTax', value='110',
        report_date='2025-06-30', filing_date='2025-08-01', accession_number='0000000001-25-000001',
        source_fact_hash='b'*64, source_url='https://www.sec.gov/Archives/edgar/data/1/000000000125000001/restated.htm')
    scope = [row, restated]
    review = dict(companyId=1, metricId=2, metric='REVENUE', start='2022-07-01', end='2023-06-30',
        chosenTag=restated['source_tag'], reviewedOn='2026-10-30', sourceScopeHash=digest(scope),
        basis='FULL_RETROSPECTIVE_REVENUE_STANDARD', value='110', originalValue='100', revenueStandardAdjustment='10',
        filingDate='2025-08-01', accessions=[restated['accession_number']], sourceFactHashes=['b'*64],
        officialDocuments=[restated['source_url']], reviewExplanation='Official note reconciles original 100 plus adjustment 10 to 110.')
    return scope, review


def test_review_is_scoped_deterministic_and_preserves_originals():
    scope, review = fixture(); before = deepcopy(scope)
    approved = reviewed_approval(scope, review)
    assert approved['company_id'] == 1 and approved['end'] == '2023-06-30'
    assert approved['proof']['originalSources'][0]['value'] == '100'
    assert approved['proof']['sources'][0]['value'] == '110'
    assert approved == reviewed_approval(scope, review) and scope == before


@pytest.mark.parametrize('field,value', [
    ('companyId', 2), ('metric', 'NET_INCOME'), ('chosenTag', 'SalesRevenueNet'),
    ('basis', 'USE_LARGEST_VALUE'), ('value', '111'), ('value', 'NaN'),
    ('sourceFactHashes', ['c'*64]), ('accessions', ['unreviewed']),
    ('revenueStandardAdjustment', '11'), ('officialDocuments', []),
    ('reviewExplanation', ''), ('filingDate', '2026-08-01')])
def test_missing_or_changed_review_does_not_approve(field, value):
    scope, review = fixture(); review[field] = value
    with pytest.raises(ValueError): reviewed_approval(scope, review)


def test_original_total_can_only_be_unchanged_when_values_match():
    scope, review = fixture(); review['basis'] = 'UNCHANGED_TOTAL_RECLASSIFICATION'
    with pytest.raises(ValueError): reviewed_approval(scope, review)
    scope[1]['value'] = '100'; review.update(value='100', sourceScopeHash=digest(scope))
    assert reviewed_approval(scope, review)['proof']['review']['basis'] == 'UNCHANGED_TOTAL_RECLASSIFICATION'


def test_new_source_requires_a_new_review_even_when_its_amount_matches():
    scope, review = fixture(); scope.append(dict(scope[1], source_fact_hash='c'*64))
    with pytest.raises(ValueError, match='STORED_SOURCE_SCOPE_CHANGED'): reviewed_approval(scope, review)


def test_unchanged_total_confirmation_does_not_require_a_reclassification_claim():
    scope, review = fixture(); review['basis'] = 'UNCHANGED_TOTAL_CONFIRMED'
    with pytest.raises(ValueError, match='UNCHANGED_TOTAL_DOES_NOT_MATCH'): reviewed_approval(scope, review)
    scope[1]['value'] = '100'; review.update(value='100', sourceScopeHash=digest(scope))
    assert reviewed_approval(scope, review)['proof']['review']['basis'] == 'UNCHANGED_TOTAL_CONFIRMED'


def test_full_retrospective_adjustment_can_reduce_revenue():
    scope, review = fixture(); scope[1]['value'] = '90'
    review.update(value='90', revenueStandardAdjustment='-10', sourceScopeHash=digest(scope))
    assert reviewed_approval(scope, review)['proof']['sources'][0]['value'] == '90'
