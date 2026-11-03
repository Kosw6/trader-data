import gzip,hashlib
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
import pytest
from pipeline.revenue_coverage694 import bound_table
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest


def fixture(tmp_path,*,split=False):
    label=['Revenues, other than interest on funds held for clients and PEO revenues','Interest on funds held for clients','PEO revenues (A)']
    first='<tr><td>'+label[0]+'</td><td>70</td><td>60</td></tr>'
    if split:first='<tr><td>Revenues, other than interest on funds</td></tr><tr><td>held for clients and PEO revenues</td><td>70</td><td>60</td></tr>'
    body=('<p>Statements of Consolidated Earnings (In millions)</p><table><tr><td>Years ended June 30,</td><td>2010</td><td>2009</td></tr>'+first+'<tr><td>'+label[1]+'</td><td>5</td><td>4</td></tr><tr><td>'+label[2]+'</td><td>15</td><td>16</td></tr><tr><td>TOTAL REVENUES</td><td>90</td><td>80</td></tr></table>').encode()
    sha=hashlib.sha256(body).hexdigest();p=tmp_path/'documents';p.mkdir();(p/(sha+'.html.gz')).write_bytes(gzip.compress(body))
    parser=Tables();parser.feed(body.decode());rows=parser.tables[0];off=int(split)
    e={'url':'https://www.sec.gov/Archives/edgar/data/1/000000000110000001/original.htm','sha256':sha,'tableHash':digest(rows),'yearRow':0,'fiscalYear':2010,'totalRow':4+off,'unitScale':1000000,'totalLabel':'TOTAL REVENUES','annualHeading':'Years ended June 30,','componentRows':[1+off,2+off,3+off],'componentLabels':label,'labelStartRows':[1,2+off,3+off]}
    entry={'documents':[{'url':e['url'],'sha256':sha}],'review':{'originalValue':'70000000'}}
    return entry,e


@pytest.mark.parametrize('split',[True,False])
def test_original_partial_line_is_distinguished_from_total_without_restatement(tmp_path,split):
    entry,e=fixture(tmp_path,split=split);r=bound_table(entry,tmp_path,e,'2010-06-30','90000000',original=True)
    assert r['componentAmountsUSD']==['70000000','5000000','15000000']
    assert r['classification']=='PARTIAL_SOURCE_TAG_VERSUS_TOTAL_REVENUE_NOT_A_RESTATEMENT'


@pytest.mark.parametrize('field,value,reason',[
    ('unitScale',1000,'COVERAGE_UNIT'),('fiscalYear',2008,'COVERAGE_YEAR_COLUMNS'),
    ('totalRow',1,'COVERAGE_TOTAL_LABEL'),('tableHash','e'*64,'COVERAGE_EXACT_TABLE_REQUIRED'),
    ('url','https://www.sec.gov/other','COVERAGE_DOCUMENT_IDENTITY'),
    ('componentRows',[1,3,2],'COVERAGE_NONCONTIGUOUS_COMPONENTS'),
    ('componentLabels',['Net sales','Interest on funds held for clients','PEO revenues (A)'],'COVERAGE_COMPONENT_LABEL'),
    ('labelStartRows',[0,2,3],'COVERAGE_COMPONENT_LABEL'),
])
def test_unbound_layout_unit_company_or_year_fails_closed(tmp_path,field,value,reason):
    entry,e=fixture(tmp_path);e[field]=value
    with pytest.raises(ValueError,match=reason):bound_table(entry,tmp_path,e,'2010-06-30','90000000',original=True)


def test_changed_total_needs_a_separate_review_not_coverage_arithmetic(tmp_path):
    entry,e=fixture(tmp_path)
    with pytest.raises(ValueError,match='COVERAGE_TOTAL_VALUE'):bound_table(entry,tmp_path,e,'2010-06-30','89000000',original=True)


def test_wrong_original_partial_amount_is_not_replaced(tmp_path):
    entry,e=fixture(tmp_path);entry['review']['originalValue']='71000000'
    with pytest.raises(ValueError,match='COVERAGE_COMPONENT_EQUATION'):bound_table(entry,tmp_path,e,'2010-06-30','90000000',original=True)


def integrated(tmp_path):
    from pipeline.revenue_coverage694 import BASIS
    entry,e=fixture(tmp_path,split=True);url=e['url'];other='https://www.sec.gov/Archives/edgar/data/1/000000000112000001/later.htm'
    a=dict(company_id=1,metric_id=2,metric_code='REVENUE',taxonomy='us-gaap',unit='USD',dimensions=None,cik='0000000001',period_start='2009-07-01',period_end='2010-06-30',form_type='10-K',source_tag='SalesRevenueNet',value='70000000',report_date='2010-06-30',filing_date='2010-08-01',accession_number='0000000001-10-000001',source_fact_hash='a'*64,source_url=url,fiscal_year=2010)
    b=dict(a,source_tag='Revenues',value='90000000',report_date='2012-06-30',filing_date='2012-08-01',accession_number='0000000001-12-000001',source_fact_hash='b'*64,source_url=other)
    rows=[a,b];entry['cik']='0000000001';entry['documents'].append(dict(entry['documents'][0],url=other))
    entry['review']=dict(companyId=1,metricId=2,metric='REVENUE',chosenTag='Revenues',start=a['period_start'],end=a['period_end'],reviewedOn='2026-10-31',sourceScopeHash=digest(rows),basis=BASIS,value='90000000',originalValue='70000000',filingDate=b['filing_date'],accessions=[b['accession_number']],sourceFactHashes=[b['source_fact_hash']],officialDocuments=[url,other],reviewExplanation='Original statement separates a named partial revenue component from total revenue; later total is unchanged.',originalCoverageEvidence=e,selectedTotalEvidence=dict(e,url=other))
    return rows,entry


def test_registered_coverage_replays_source_documents_and_preserves_both_values(tmp_path):
    from pipeline.financial_reconciliation677 import validate_entry
    from pipeline.revenue_coverage694 import POLICY,BASIS
    rows,entry=integrated(tmp_path);before=deepcopy(rows);a=validate_entry(rows,entry,tmp_path)
    assert a['proof']['policy']==POLICY and a['proof']['review']['basis']==BASIS
    assert a['proof']['restatementClaimed'] is False and 'revenueStandardAdjustment' not in a['proof']['review']
    assert a['proof']['originalSources'][0]['value']=='70000000' and rows==before
    assert a['proof_hash']==validate_entry(rows,entry,tmp_path)['proof_hash']


@pytest.mark.parametrize('change',['raw_value','original_document','selected_document','fiscal_year','original_partial'])
def test_registered_coverage_cannot_change_source_or_filing_identity(tmp_path,change):
    from pipeline.financial_reconciliation677 import validate_entry
    rows,entry=integrated(tmp_path)
    if change=='raw_value':rows[0]['value']='71000000'
    if change=='original_document':entry['review']['originalCoverageEvidence']['url']=entry['review']['selectedTotalEvidence']['url']
    if change=='selected_document':entry['review']['selectedTotalEvidence']['url']=entry['review']['originalCoverageEvidence']['url']
    if change=='fiscal_year':entry['review']['originalCoverageEvidence']['fiscalYear']=2009
    if change=='original_partial':entry['review']['originalValue']='71000000'
    with pytest.raises(ValueError):validate_entry(rows,entry,tmp_path)
