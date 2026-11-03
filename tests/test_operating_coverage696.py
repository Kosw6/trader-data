import gzip,hashlib
from copy import deepcopy
import pytest
from pipeline.revenue_coverage694 import bound_table,BASIS
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.financial_reconciliation677 import validate_entry


def fixture(tmp_path):
    body=b'<p>CONSOLIDATED STATEMENTS OF INCOME (in millions)</p><table><tr><td>For the year ended December 31,</td><td>2013</td><td>2012</td></tr><tr><td>Revenues:</td></tr><tr><td>Premiums</td><td>39</td><td>37</td></tr><tr><td>Services</td><td>2</td><td>2</td></tr><tr><td>Investment income</td><td>1</td><td>1</td></tr><tr><td></td></tr><tr><td>Total revenues</td><td>42</td><td>40</td></tr></table>'
    sha=hashlib.sha256(body).hexdigest();(tmp_path/'documents').mkdir();(tmp_path/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode())
    url='https://www.sec.gov/Archives/edgar/data/1/000000000114000001/original.htm';later='https://www.sec.gov/Archives/edgar/data/1/000000000116000001/later.htm'
    e=dict(url=url,sha256=sha,tableHash=digest(p.tables[0]),yearRow=0,totalRow=6,unitScale=1000000,fiscalYear=2013,totalLabel='Total revenues',annualHeading='For the year ended December 31,',componentRows=[2,3,4],componentLabels=['Premiums','Services','Investment income'],componentCoverageContract='OPERATING_REVENUE_PLUS_INVESTMENT696_V1',revenueSectionRow=1)
    a=dict(company_id=1,metric_id=2,metric_code='REVENUE',taxonomy='us-gaap',unit='USD',dimensions=None,cik='1',period_start='2013-01-01',period_end='2013-12-31',form_type='10-K',source_tag='SalesRevenueNet',value='41000000',report_date='2013-12-31',filing_date='2014-02-01',accession_number='0000000001-14-000001',source_fact_hash='a'*64,source_url=url,fiscal_year=2013)
    b=dict(a,source_tag='Revenues',value='42000000',report_date='2015-12-31',filing_date='2016-02-01',accession_number='0000000001-16-000001',source_fact_hash='b'*64,source_url=later)
    rows=[a,b];r=dict(companyId=1,metricId=2,metric='REVENUE',chosenTag='Revenues',start=a['period_start'],end=a['period_end'],reviewedOn='2026-10-31',sourceScopeHash=digest(rows),basis=BASIS,value='42000000',originalValue='41000000',filingDate=b['filing_date'],accessions=[b['accession_number']],sourceFactHashes=[b['source_fact_hash']],officialDocuments=[url,later],reviewExplanation='Operating premiums plus services are a subset of total revenue including investment income; unchanged total in both statements.',originalCoverageEvidence=e,selectedTotalEvidence=dict(e,url=later))
    return rows,{'cik':'1','review':r,'documents':[dict(url=url,sha256=sha),dict(url=later,sha256=sha)]}


def test_premium_and_service_subset_is_explicit_not_an_inferred_restatement(tmp_path):
    rows,e=fixture(tmp_path);original=deepcopy(rows);a=validate_entry(rows,e,tmp_path)
    assert a['proof']['policy']=='sec-reviewed-compound-operating-revenue696-v1'
    assert a['proof']['originalStatement']['originalSubsetUSD']=='41000000'
    assert a['proof']['originalStatement']['componentAmountsUSD']==['39000000','2000000','1000000']
    assert not a['proof']['restatementClaimed'] and rows==original


@pytest.mark.parametrize('field,value',[
    ('componentLabels',['Services','Premiums','Investment income']),
    ('componentRows',[2,3,5]),('revenueSectionRow',0),('componentCoverageContract','OTHER'),
    ('fiscalYear',2012),('unitScale',1000),('tableHash','e'*64),
])
def test_changed_components_period_unit_or_section_are_rejected(tmp_path,field,value):
    rows,e=fixture(tmp_path);e['review']['originalCoverageEvidence'][field]=value
    with pytest.raises(ValueError):validate_entry(rows,e,tmp_path)
