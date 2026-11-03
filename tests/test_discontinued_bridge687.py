import gzip,hashlib
from copy import deepcopy
import pytest
from tests.test_financial_reconciliation677 import fixture
from pipeline.financial_reconciliation677 import validate_entry
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.reconciliation_review679 import Tables
from pipeline.discontinued_bridge687 import BASIS,POLICY,representation_cause


def case(tmp_path):
    scope,e,_=fixture(tmp_path);scope[1]['value']='90'
    for r in scope:r['fiscal_year']=2023
    review=e['review'];review.update(basis=BASIS,value='90',discontinuedRevenue='10',sourceScopeHash=digest(scope))
    cause='The disposed business was reclassified as discontinued operations for all periods presented.'
    context='The results of discontinued operations follow (in thousands).'
    body=('<p>'+cause+'</p><p>'+context+'</p><table><tr><td>2024</td><td>2023</td><td>2022</td></tr><tr><td>Net sales</td><td>0.012</td><td>0.010</td><td>0.008</td></tr></table>').encode()
    sha=hashlib.sha256(body).hexdigest()
    with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
    e['documents'][1]['sha256']=sha;p=Tables();p.feed(body.decode())
    review['discontinuedEvidence']={'url':scope[1]['source_url'],'sha256':sha,'tableHash':digest(p.tables[0]),'yearRow':0,'valueRow':1,'rowLabel':'Net sales','unitScale':1000,'fiscalYear':2023,'causeExcerpt':cause,'tableContextExcerpt':context}
    return scope,e


def test_exact_published_bridge_has_separate_cause_and_never_mutates_source(tmp_path):
    scope,e=case(tmp_path);before=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
    assert a['proof']['policy']==POLICY and a['proof']['accountingCause']==BASIS and scope==before
    assert a['proof']['bridge']['samePeriodTotal']=='100'


@pytest.mark.parametrize('field,value',[
 ('yearRow',1),('valueRow',0),('fiscalYear',2022),('rowLabel','Income tax'),('unitScale',1000000),
 ('causeExcerpt','The company discussed discontinued operations in an unrelated period.'),
 ('tableContextExcerpt','The continuing operations are presented in thousands.'),('tableHash','0'*64),
 ('url','https://evil.invalid'),('sha256','0'*64)])
def test_wrong_component_period_cause_or_source_never_approves(tmp_path,field,value):
    scope,e=case(tmp_path);e['review']['discontinuedEvidence'][field]=value
    with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)


@pytest.mark.parametrize('removed',['-10','9','NaN'])
def test_no_inferred_or_nonfinite_component(tmp_path,removed):
    scope,e=case(tmp_path);e['review']['discontinuedRevenue']=removed
    with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)


def test_missing_original_is_not_replaced_by_a_component_table(tmp_path):
    scope,e=case(tmp_path);scope=scope[1:];e['review']['sourceScopeHash']=digest(scope)
    with pytest.raises(ValueError,match='ORIGINAL'):validate_entry(scope,e,tmp_path)


def test_derivative_income_reclassification_is_not_business_revenue_representation():
    assert not representation_cause('Foreign currency derivatives were reclassified from AOCI into income from discontinued operations in 2023.')
    assert representation_cause('For all periods, all sales of the disposed business have been reported under income from discontinued operations.')


def causal_case(tmp_path):
    scope,e=case(tmp_path);r=e['review'];ev=r['discontinuedEvidence'];middle={**scope[1],'filing_date':'2024-08-01','report_date':'2024-06-30','accession_number':'0000000001-24-000001','source_fact_hash':'c'*64,'source_url':'https://www.sec.gov/Archives/edgar/data/1/000000000124000001/causal.htm'};scope.append(middle)
    body=('<p>'+ev['causeExcerpt']+'</p>').encode();sha=hashlib.sha256(body).hexdigest()
    with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
    e['documents'].append({'url':middle['source_url'],'sha256':sha});r['officialDocuments'].append(middle['source_url']);r['sourceScopeHash']=digest(scope);ev['causeDocument']={'url':middle['source_url'],'sha256':sha}
    return scope,e


def test_prior_actual_reclassification_can_confirm_identical_latest_comparative(tmp_path):
    scope,e=causal_case(tmp_path);assert validate_entry(scope,e,tmp_path)['proof']['policy']==POLICY


@pytest.mark.parametrize('change',['value','tag','later','other_document'])
def test_prior_cause_cannot_license_a_different_or_unrelated_latest_value(tmp_path,change):
    scope,e=causal_case(tmp_path)
    if change=='value':scope[-1]['value']='89'
    if change=='tag':scope[-1]['source_tag']='SalesRevenueNet'
    if change=='later':scope[-1]['filing_date']='2026-01-01'
    if change=='other_document':e['review']['discontinuedEvidence']['causeDocument']['url']=scope[0]['source_url']
    e['review']['sourceScopeHash']=digest(scope)
    with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)


@pytest.mark.parametrize('display,accepted',[('\u2014',True),('',False),('-',False),('n/a',False)])
def test_explicit_zero_preserves_year_column_without_filling_missing_values(tmp_path,display,accepted):
    scope,e=case(tmp_path);ev=e['review']['discontinuedEvidence']
    with gzip.open(tmp_path/'documents'/(ev['sha256']+'.html.gz'),'rb') as f:body=f.read()
    body=body.replace(b'<td>0.012</td>',('<td>'+display+'</td>').encode())
    sha=hashlib.sha256(body).hexdigest()
    with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
    e['documents'][1]['sha256']=sha;ev['sha256']=sha;p=Tables();p.feed(body.decode());ev['tableHash']=digest(p.tables[0])
    before=deepcopy(scope)
    if accepted:assert validate_entry(scope,e,tmp_path)['proof']['bridge']['latestDiscontinuedRevenue']=='10'
    else:
        with pytest.raises(ValueError,match='COLUMN_COUNT'):validate_entry(scope,e,tmp_path)
    assert scope==before
