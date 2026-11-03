import gzip
import hashlib
from copy import deepcopy
import pytest
from tests.test_financial_reconciliation677 import fixture
from pipeline.financial_reconciliation677 import validate_entry
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.reconciliation_review679 import Tables
from pipeline.bound_reconciliation685 import STANDARD, AMENDMENT, ERROR, POLICY, period_years


def case(tmp_path, amendment=False, multi=False, negative=False):
    scope, entry, _ = fixture(tmp_path)
    for x in scope: x['fiscal_year'] = 2023
    r = entry['review']; r['basis'] = AMENDMENT if amendment else STANDARD
    if negative:
        scope[1]['value'] = r['value'] = '90'
    if amendment:
        amended = dict(scope[0], value='110', form_type='10-K/A', filing_date='2023-09-01',
                       accession_number='0000000001-23-000002', source_fact_hash='c'*64,
                       source_url='https://www.sec.gov/Archives/edgar/data/1/000000000123000002/amended.htm')
        scope.append(amended); r['originalValue'] = '110'
        cause = '(a) Customer rebates caused a misstatement of net sales in 2023.'
        url = amended['source_url']
    else:
        cause = 'The Company adopted the new revenue standard using the full retrospective method for all periods.'
        url = scope[1]['source_url']
    delta = '(10)' if negative else '10'
    body = ('<p>'+cause+'</p><table><tr><td>Year ended June 30, 2023</td>'+
            ('<td>2022</td>' if multi else '')+'</tr><tr><td>(in USD)</td></tr>'
            '<tr><td>As reported</td><td>Adjustment</td><td>As restated</td></tr>'
            '<tr><td>Net sales</td><td>100</td><td>'+delta+'</td><td>'+r['value']+'</td>'+
            ('<td>(a)</td>' if amendment else '')+
            ('<td>80</td><td>2</td><td>82</td>' if multi else '')+'</tr></table>').encode()
    sha = hashlib.sha256(body).hexdigest()
    with gzip.open(tmp_path/'documents'/(sha+'.html.gz'), 'wb') as f: f.write(body)
    entry['documents'] = [d for d in entry['documents'] if d['url'] != url]+[{'url':url, 'sha256':sha}]
    r['officialDocuments'] = [d['url'] for d in entry['documents']]
    p = Tables(); p.feed(body.decode())
    r['boundTableEvidence'] = {'url':url, 'sha256':sha, 'tableHash':digest(p.tables[0]),
      'yearRow':0, 'valueRow':3, 'rowLabel':'Net sales', 'unitScale':1, 'fiscalYear':2023,
      'orderedHeaders':['As reported','Adjustment','As restated'], 'causeExcerpt':cause,
      'reference':'(a)' if amendment else ''}
    r['boundTableEvidence']['filingDate']=next(x['filing_date'] for x in scope if x['source_url']==url)
    r['sourceScopeHash'] = digest(scope)
    return scope, entry


@pytest.mark.parametrize('amendment,multi,negative',[(False,False,False),(False,True,False),(False,True,True),(True,False,False)])
def test_bound_table_and_complete_scope_replay_without_mutation(tmp_path, amendment, multi, negative):
    scope,e=case(tmp_path,amendment,multi,negative);before=deepcopy(scope)
    a=validate_entry(scope,e,tmp_path)
    assert a['proof']['policy']==POLICY and a['proof']['review']['basis']==e['review']['basis']
    assert scope==before and a['proof_hash']==validate_entry(scope,e,tmp_path)['proof_hash']


@pytest.mark.parametrize('field,value',[
 ('fiscalYear',2022),('yearRow',3),('valueRow',0),('rowLabel','Net income'),
 ('unitScale',1000),('causeExcerpt','The Company adopted an unrelated standard using full retrospective method.'),
 ('orderedHeaders',['As reported','As restated','Adjustment']),('tableHash','0'*64),
 ('url','https://evil.invalid'),('sha256','0'*64),('filingDate','2020-01-01')])
def test_changed_bound_evidence_is_rejected(tmp_path,field,value):
    scope,e=case(tmp_path);e['review']['boundTableEvidence'][field]=value
    with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)


def test_original_fact_change_cannot_be_hidden_by_matching_table(tmp_path):
    scope,e=case(tmp_path);scope[0]['value']='99'
    with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)


def test_amendment_requires_original_report_and_matching_reference(tmp_path):
    scope,e=case(tmp_path,True);e['review']['boundTableEvidence']['reference']='(b)'
    with pytest.raises(ValueError,match='REFERENCE'):validate_entry(scope,e,tmp_path)


def test_amendment_does_not_license_new_changed_value(tmp_path):
    scope,e=case(tmp_path,True);scope[-1]['value']='109';e['review']['originalValue']='109';e['review']['sourceScopeHash']=digest(scope)
    with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)


def components(tmp_path, context_period=False):
    scope,e=case(tmp_path);b=e['review']['boundTableEvidence'];cause=b['causeExcerpt']
    heading='<p>for the fiscal year ended June 30, 2023</p>' if context_period else ''
    body=('<p>'+cause+'</p>'+heading+'<table><tr><td>'+('Details' if context_period else 'Year ended June 30, 2023')+'</td></tr>'
          '<tr><td>(in USD)</td></tr><tr><td>As reported</td><td>Adjustment</td><td>As restated</td></tr>'
          '<tr><td>Revenue standard</td><td>Pension</td></tr>'
          '<tr><td>Net sales</td><td>100</td><td>10</td><td>—</td><td>110</td></tr></table>').encode()
    sha=hashlib.sha256(body).hexdigest()
    with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
    next(d for d in e['documents'] if d['url']==b['url'])['sha256']=sha
    p=Tables();p.feed(body.decode());b.update(sha256=sha,tableHash=digest(p.tables[0]),valueRow=4,
      yearRow=None if context_period else 0,componentLabels=['Revenue standard','Pension'],componentAmountsUSD=['10','0'])
    if context_period:b['annualContextHeading']='for the fiscal year ended June 30, 2023'
    return scope,e


@pytest.mark.parametrize('context_period',[False,True])
def test_explicit_zero_components_and_annual_context_are_bound(tmp_path,context_period):
    scope,e=components(tmp_path,context_period)
    assert validate_entry(scope,e,tmp_path)['proof']['tableAmountsUSD']==['100','10','110']


@pytest.mark.parametrize('change',[
 {'componentLabels':['Pension','Revenue standard']}, {'componentAmountsUSD':['0','10']},
 {'componentAmountsUSD':['10','1']}, {'componentLabels':[]},
 {'annualContextHeading':'for the fiscal year ended June 30, 2022'},
 {'annualContextHeading':'for the year ended June 30, 2023'}])
def test_ambiguous_or_changed_component_bindings_never_approve(tmp_path,change):
    scope,e=components(tmp_path,True);e['review']['boundTableEvidence'].update(change)
    with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)


def test_accounting_standard_number_is_not_a_period_column():
    assert period_years(['ASU 2017-07','ASC Topic 606','2017-01'])==[]
    assert period_years(['Fiscal 2017','December 31, 2016'])==['2017','2016']


@pytest.mark.parametrize('header,valid',[('AsPreviously Reported',True),('AsPreviouslyReported',True),('AsOtherReported',False)])
def test_known_sec_header_spacing_is_bound_without_changing_numbers(tmp_path,header,valid):
    scope,e=case(tmp_path);b=e['review']['boundTableEvidence'];file=tmp_path/'documents'/(b['sha256']+'.html.gz');body=gzip.decompress(file.read_bytes()).replace(b'As reported',header.encode());sha=hashlib.sha256(body).hexdigest()
    with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
    next(d for d in e['documents'] if d['url']==b['url'])['sha256']=sha
    p=Tables();p.feed(body.decode());b.update(sha256=sha,tableHash=digest(p.tables[0]),orderedHeaders=[header,'Adjustment','As restated'])
    if valid:assert validate_entry(scope,e,tmp_path)['proof']['policy']==POLICY
    else:
        with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)


@pytest.mark.parametrize('year,valid',[('2023',True),('2022',False)])
def test_explicit_error_table_requires_its_own_year_cause_and_stays_separate_from_revenue_standard(tmp_path,year,valid):
    scope,e=case(tmp_path);r=e['review'];b=r['boundTableEvidence'];cause='The correction of errors required restatement of revenue for '+year+'.'
    file=tmp_path/'documents'/(b['sha256']+'.html.gz');body=gzip.decompress(file.read_bytes()).replace(b['causeExcerpt'].encode(),cause.encode());sha=hashlib.sha256(body).hexdigest()
    with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
    next(d for d in e['documents'] if d['url']==b['url'])['sha256']=sha;b.update(sha256=sha,causeExcerpt=cause);r['basis']=ERROR
    if valid:
        a=validate_entry(scope,e,tmp_path);assert a['proof']['accountingCause']==ERROR and a['proof']['review']['basis']==ERROR
    else:
        with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)


@pytest.mark.parametrize('error_basis,correct_year',[(True,True),(True,False),(False,True)])
def test_as_revised_requires_explicit_annual_error_correction_not_standard_adoption(tmp_path,error_basis,correct_year):
    scope,e=case(tmp_path);r=e['review'];b=r['boundTableEvidence']
    cause='The Company revised the annual revenue to correct other misstatements for '+('2023' if correct_year else '2022')+'.'
    file=tmp_path/'documents'/(b['sha256']+'.html.gz');body=gzip.decompress(file.read_bytes()).replace(b['causeExcerpt'].encode(),cause.encode()).replace(b'As restated',b'As Revised');sha=hashlib.sha256(body).hexdigest()
    with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
    next(d for d in e['documents'] if d['url']==b['url'])['sha256']=sha;p=Tables();p.feed(body.decode())
    b.update(sha256=sha,tableHash=digest(p.tables[0]),causeExcerpt=cause,orderedHeaders=['As reported','Adjustment','As Revised']);r['basis']=ERROR if error_basis else STANDARD
    if error_basis and correct_year:assert validate_entry(scope,e,tmp_path)['proof']['accountingCause']==ERROR
    else:
        with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)
