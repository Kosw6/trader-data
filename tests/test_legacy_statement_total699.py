import gzip
import hashlib
from copy import deepcopy
import pytest
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest, reviewed_approval
from pipeline.legacy_statement_total699 import BASIS, POLICY, statement
from pipeline.financial_reconciliation677 import validate_entry


def fixture(tmp_path, mode='YEAR'):
    def doc(name, years, *, full_dates=False):
        header=['June 30, '+str(y) for y in years] if full_dates else years
        body=('<p>Consolidated Statements of Operations (in thousands)</p><table><tr><td>For the years ended June 30,</td>'+''.join('<td>'+str(v)+'</td>' for v in header)+'</tr><tr><td>Net sales</td>'+''.join('<td>'+str(v)+'</td>' for v in ([70000,60000] if len(years)==2 else [90000,80000,70000]))+'</tr><tr><td>Cost of sales</td>'+''.join('<td>30000</td>' for _ in years)+'</tr></table>').encode()
        sha=hashlib.sha256(body).hexdigest();folder=tmp_path/'documents';folder.mkdir(exist_ok=True);(folder/(sha+'.html.gz')).write_bytes(gzip.compress(body))
        p=Tables();p.feed(body.decode())
        return dict(url='https://www.sec.gov/Archives/edgar/data/1/'+name+'/annual.htm',sha256=sha,tableHash=digest(p.tables[0]),periodRow=0,valueRow=1,rowLabel='Net sales',unitScale=1000,periodMode=mode,annualHeading='For the years ended June 30,')
    a=doc('000000000110000001',[2010,2009],full_dates=mode=='DATE');b=doc('000000000112000001',[2012,2011,2010],full_dates=mode=='DATE')
    first=dict(company_id=1,metric_id=2,metric_code='REVENUE',taxonomy='us-gaap',unit='USD',dimensions=None,cik='0000000001',period_start='2009-07-01',period_end='2010-06-30',form_type='10-K',source_tag='SalesRevenueNet',value='70000000',report_date='2010-06-30',filing_date='2010-08-01',accession_number='0000000001-10-000001',source_fact_hash='a'*64,source_url=a['url'],fiscal_year=2010)
    last=dict(first,report_date='2012-06-30',filing_date='2012-08-01',accession_number='0000000001-12-000001',source_fact_hash='b'*64,source_url=b['url'])
    modern=dict(last,source_tag='Revenues',value='5000000',source_fact_hash='c'*64)
    rows=[first,last,modern]
    r=dict(companyId=1,metricId=2,metric='REVENUE',chosenTag='SalesRevenueNet',start='2009-07-01',end='2010-06-30',reviewedOn='2026-10-31',sourceScopeHash=digest(rows),basis=BASIS,value='70000000',originalValue='70000000',filingDate='2012-08-01',accessions=[last['accession_number']],sourceFactHashes=[last['source_fact_hash']],officialDocuments=[a['url'],b['url']],conflictingModernTag='Revenues',conflictingModernValue='5000000',conflictingModernFactHashes=['c'*64],conflictingModernAccessions=[last['accession_number']],conflictingModernFilingDate='2012-08-01',statementTotalEvidence=dict(original=a,latestLegacy=b,modernReport=dict(b)),reviewExplanation='Original and both latest source reports independently confirm the full consolidated statement total. This is a company/period-only registered choice, not an unrestricted source alias.')
    entry=dict(cik='0000000001',review=r,documents=[dict(url=v['url'],sha256=v['sha256']) for v in [a,b]])
    return rows,entry


@pytest.mark.parametrize('mode',['YEAR','DATE'])
def test_exact_document_totals_select_real_legacy_tag_and_preserve_modern_raw(tmp_path,mode):
    rows,entry=fixture(tmp_path,mode);before=deepcopy(rows);a=validate_entry(rows,entry,tmp_path)
    assert rows==before and a['source_tag']=='SalesRevenueNet' and a['proof']['policy']==POLICY
    assert a['proof']['conflictingModernSources'][0]['value']=='5000000'
    assert a['proof']['restatementClaimed'] is False
    assert a['proof_hash']==validate_entry(rows,entry,tmp_path)['proof_hash']


def test_ordinary_672_policy_cannot_choose_legacy_source(tmp_path):
    rows,entry=fixture(tmp_path)
    with pytest.raises(ValueError,match='UNREVIEWED_METRIC_OR_TAG'):
        reviewed_approval(rows,{**entry['review'],'basis':'UNCHANGED_TOTAL_CONFIRMED'})
    with pytest.raises(ValueError,match='LEGACY_STATEMENT_IDENTITY_CONTRACT_REQUIRED'):
        reviewed_approval(rows,entry['review'],_reviewed_legacy_total=True)


@pytest.mark.parametrize('change',[
    'scope_hash','source_value','selected_hash','chosen_modern_tag','original_url','latest_url',
    'modern_url','modern_value','modern_hash','modern_accession','modern_date','legacy_changed',
    'foreign_form','quarter_form','invalid_source_url','missing_hash','negative_date','unsupported_tag',
    'different_company','different_unit','dimensions','nonfinite_modern',
])
def test_source_identity_and_unreviewed_semantics_fail_closed(tmp_path,change):
    rows,entry=fixture(tmp_path);r=entry['review'];e=r['statementTotalEvidence']
    if change=='scope_hash':r['sourceScopeHash']='d'*64
    elif change=='source_value':rows[0]['value']='71000000'
    elif change=='selected_hash':r['sourceFactHashes']=['d'*64]
    elif change=='chosen_modern_tag':r['chosenTag']='Revenues'
    elif change=='original_url':e['original']['url']=e['latestLegacy']['url']
    elif change=='latest_url':e['latestLegacy']['url']=e['original']['url']
    elif change=='modern_url':e['modernReport']['url']=e['original']['url']
    elif change=='modern_value':r['conflictingModernValue']='70000000'
    elif change=='modern_hash':r['conflictingModernFactHashes']=['d'*64]
    elif change=='modern_accession':r['conflictingModernAccessions']=['0000000001-11-000001']
    elif change=='modern_date':r['conflictingModernFilingDate']='2012-09-01'
    else:
        if change=='legacy_changed':rows[1]['value']='71000000'
        elif change=='foreign_form':rows[2]['form_type']='20-F'
        elif change=='quarter_form':rows[2]['form_type']='10-Q'
        elif change=='invalid_source_url':rows[2]['source_url']='https://example.com/raw'
        elif change=='missing_hash':rows[2]['source_fact_hash']=''
        elif change=='negative_date':rows[2]['filing_date']='2009-01-01'
        elif change=='unsupported_tag':rows[2]['source_tag']='LicenseRevenue'
        elif change=='different_company':rows[2]['company_id']=2
        elif change=='different_unit':rows[2]['unit']='EUR'
        elif change=='dimensions':rows[2]['dimensions']={'segment':'subset'}
        elif change=='nonfinite_modern':rows[2]['value']='NaN'
        r['sourceScopeHash']=digest(rows)
    with pytest.raises(ValueError):validate_entry(rows,entry,tmp_path)


@pytest.mark.parametrize('field,value',[
    ('periodRow',1),('valueRow',2),('rowLabel','Segment sales'),('unitScale',1000000),
    ('periodMode','FISCAL'),('annualHeading','For the three months ended June 30,'),
    ('tableHash','d'*64),
])
def test_statement_binding_rejects_wrong_period_unit_label(tmp_path,field,value):
    rows,entry=fixture(tmp_path);entry['review']['statementTotalEvidence']['original'][field]=value
    with pytest.raises(ValueError):validate_entry(rows,entry,tmp_path)


def test_date_columns_cannot_be_reinterpreted_as_fiscal_year_only(tmp_path):
    rows,entry=fixture(tmp_path,'DATE');entry['review']['statementTotalEvidence']['original']['periodMode']='YEAR'
    with pytest.raises(ValueError,match='LEGACY_TOTAL_FULL_DATE_AS_YEAR_NOT_ALLOWED'):validate_entry(rows,entry,tmp_path)


def rewrite(entry, root, binding, old, new):
    oldsha=binding['sha256'];url=binding['url']
    text=gzip.decompress((root/'documents'/(oldsha+'.html.gz')).read_bytes()).decode();assert old in text
    body=text.replace(old,new,1).encode();sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body))
    p=Tables();p.feed(body.decode());assert len(p.tables)==1
    for b in entry['review']['statementTotalEvidence'].values():
        if b['url']==url:b.update(sha256=sha,tableHash=digest(p.tables[0]))
    next(d for d in entry['documents'] if d['url']==url)['sha256']=sha


@pytest.mark.parametrize('old,new,reason',[
    ('Consolidated Statements of Operations','Business segment operations','CONSOLIDATED_STATEMENT_REQUIRED'),
    ('<td>70000</td>','<td>71000</td>','STATEMENT_VALUE'),
    ('in thousands','in millions','UNIT'),
    ('<td>Net sales</td>','<td>Segment sales</td>','FULL_REVENUE_LABEL_REQUIRED'),
    ('<td>Cost of sales</td>','<td>Revenues</td>','COMPETING_STATEMENT_TOTAL'),
])
def test_semantics_are_rechecked_after_valid_document_and_table_hash_changes(tmp_path,old,new,reason):
    rows,entry=fixture(tmp_path)
    if reason=='COMPETING_STATEMENT_TOTAL':
        rewrite(entry,tmp_path,entry['review']['statementTotalEvidence']['original'],'<td>30000</td>','<td>5000</td>')
    rewrite(entry,tmp_path,entry['review']['statementTotalEvidence']['original'],old,new)
    with pytest.raises(ValueError,match=reason):validate_entry(rows,entry,tmp_path)


def test_parsing_cache_cannot_hide_a_changed_document(tmp_path):
    rows,entry=fixture(tmp_path);validate_entry(rows,entry,tmp_path)
    b=entry['review']['statementTotalEvidence']['original'];path=tmp_path/'documents'/(b['sha256']+'.html.gz');body=gzip.decompress(path.read_bytes());path.write_bytes(gzip.compress(body.replace(b'70000',b'71000')))
    with pytest.raises(ValueError,match='DOCUMENT_CHANGED'):validate_entry(rows,entry,tmp_path)


def related_fixture(tmp_path):
    rows,entry=fixture(tmp_path)
    for role in ['original','latestLegacy']:
        b=entry['review']['statementTotalEvidence'][role];years=2 if role=='original' else 3
        total=[70000,60000] if years==2 else [90000,80000,70000]
        row='<tr><td>Net sales</td>'+''.join('<td>'+str(v)+'</td>' for v in total)+'</tr>'
        replacement='<tr><td>Net sales</td></tr><tr><td>Unrelated parties</td>'+''.join('<td>'+str(v-10000)+'</td>' for v in total)+'</tr><tr><td>Related parties</td>'+''.join('<td>10000</td>' for _ in total)+'</tr><tr><td></td>'+''.join('<td>'+str(v)+'</td>' for v in total)+'</tr>'
        rewrite(entry,tmp_path,b,row,replacement)
        for node in entry['review']['statementTotalEvidence'].values():
            if node['url']==b['url']:node.update(valueRow=4,rowLabel='',totalContract='NET_SALES_RELATED_PARTY_SUM699_V1')
    return rows,entry


def test_related_party_sum_is_bound_as_one_full_total(tmp_path):
    rows,entry=related_fixture(tmp_path)
    assert validate_entry(rows,entry,tmp_path)['source_tag']=='SalesRevenueNet'


@pytest.mark.parametrize('old,new',[
    ('<td>Related parties</td>','<td>Some other sales</td>'),
    ('<td>10000</td>','<td>11000</td>'),
    ('<tr><td>Net sales</td></tr>','<tr><td>Net sales</td><td>123</td></tr>'),
])
def test_related_party_contract_rejects_wrong_labels_values_or_double_counting(tmp_path,old,new):
    rows,entry=related_fixture(tmp_path);rewrite(entry,tmp_path,entry['review']['statementTotalEvidence']['original'],old,new)
    with pytest.raises(ValueError):validate_entry(rows,entry,tmp_path)


def annual_date_fixture(tmp_path):
    rows,entry=fixture(tmp_path)
    for role,years in [('original',[2010,2009]),('latestLegacy',[2012,2011,2010])]:
        b=entry['review']['statementTotalEvidence'][role]
        heading='For the years ended '+', '.join('June 30, '+str(y) for y in years)
        rewrite(entry,tmp_path,b,'<p>','<p>'+heading+' ')
        for node in entry['review']['statementTotalEvidence'].values():
            if node['url']==b['url']:node.update(periodMode='ANNUAL_HEADER_DATES',annualDateHeading=heading,targetFiscalYear=2010)
    return rows,entry


def test_actual_annual_date_header_is_bound_to_fiscal_columns(tmp_path):
    rows,entry=annual_date_fixture(tmp_path)
    assert validate_entry(rows,entry,tmp_path)['proof']['statementTotals']['modernReport']['periodMode']=='ANNUAL_HEADER_DATES'


@pytest.mark.parametrize('change',['fiscal_column','missing_header','quarter_spacing','original_fiscal_year'])
def test_annual_fiscal_date_bridge_does_not_infer_missing_dates(tmp_path,change):
    rows,entry=annual_date_fixture(tmp_path);b=entry['review']['statementTotalEvidence']['original']
    if change=='fiscal_column':b['targetFiscalYear']=2009
    elif change=='missing_header':b['annualDateHeading']='For the years ended June 30, 2011, June 30, 2010'
    elif change=='quarter_spacing':
        old=b['annualDateHeading'];new=old.replace('June 30, 2009','March 31, 2010');rewrite(entry,tmp_path,b,old,new);b['annualDateHeading']=new
    elif change=='original_fiscal_year':rows[0]['fiscal_year']=2011;entry['review']['sourceScopeHash']=digest(rows)
    with pytest.raises(ValueError):validate_entry(rows,entry,tmp_path)


def test_audited_results_of_operations_title_and_year_ended_hyphen(tmp_path):
    rows,entry=fixture(tmp_path,'DATE')
    for role in ['original','latestLegacy']:
        b=entry['review']['statementTotalEvidence'][role]
        rewrite(entry,tmp_path,b,'Consolidated Statements of Operations','Consolidated Results of Operations')
        rewrite(entry,tmp_path,b,'For the years ended','Year-Ended')
    assert validate_entry(rows,entry,tmp_path)['proof']['policy']==POLICY


def test_explicit_annual_month_day_without_comma_keeps_real_date_binding(tmp_path):
    rows,entry=fixture(tmp_path)
    for role in ['original','latestLegacy']:
        b=entry['review']['statementTotalEvidence'][role]
        rewrite(entry,tmp_path,b,'For the years ended June 30,','For the years ended June 30')
        for node in entry['review']['statementTotalEvidence'].values():
            if node['url']==b['url']:node['annualHeading']='For the years ended June 30'
    assert validate_entry(rows,entry,tmp_path)['source_tag']=='SalesRevenueNet'
