import gzip
import hashlib
from copy import deepcopy
from decimal import Decimal
import pytest
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.revenue_bridge698 import NESTED, PRECISE, nested, detail, rounded_summary
from pipeline.financial_reconciliation677 import validate_entry


def fixture(tmp_path, basis=NESTED):
    def register(body, name, yearrow, valuerow, scale):
        body=body.encode();sha=hashlib.sha256(body).hexdigest()
        folder=tmp_path/'documents';folder.mkdir(exist_ok=True)
        (folder/(sha+'.html.gz')).write_bytes(gzip.compress(body))
        p=Tables();p.feed(body.decode())
        return dict(url='https://www.sec.gov/Archives/edgar/data/1/'+name+'/report.htm',sha256=sha,tableHash=digest(p.tables[0]),yearRow=yearrow,valueRow=valuerow,unitScale=scale,fiscalYear=2018,annualHeading='for the years ended December 31,')
    def tr(label,*v):return '<tr>'+''.join('<td>'+str(x)+'</td>' for x in [label,*v])+'</tr>'
    heading='<p>CONSOLIDATED STATEMENTS OF OPERATIONS for the years ended December 31, (in thousands)</p>'
    if basis==NESTED:
        body=heading+'<table>'+tr('',2018,2017)+tr('')+tr('Revenue:')+tr('Asset liquidation')+tr('Asset sales',20,30)+tr('Commissions and other',60,50)+tr('Total asset liquidation revenue',80,80)+tr('Intellectual property licensing',9,'—')+tr('Total revenue',89,80)+'</table>'
        original=register(body,'000000000118000001',0,8,1000)
        selected=register(body,'000000000119000001',0,8,1000)
        nodes=dict(originalStatement=original,selectedStatement=selected);a,b='80000','89000'
    else:
        body=heading+'<table>'+tr('',2018,2017)+tr('Net sales',173558,176696)+'</table>'
        original=register(body,'000000000118000001',0,1,1000)
        summary='<p>Consolidated net sales for the years ended December 31, (in millions)</p><table>'+tr('',2018,2017)+tr('Consolidated product sales','170.2','157.9')+tr('Consolidated pack sales and associate fees(a)','2.5','14.2')+tr('Consolidated other, including freight','0.9','4.6')+tr('Total','173.6','176.7')+'</table>'
        rounded=register(summary,'000000000118000001',0,4,1000000);rounded['decimalPlaces']=1
        # Both original tables reside in the SAME original document.
        combined=(body+summary).encode();sha=hashlib.sha256(combined).hexdigest();(tmp_path/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(combined))
        original['sha256']=rounded['sha256']=sha
        selected=register(body,'000000000119000001',0,1,1000)
        nodes=dict(originalStatement=original,originalSummary=rounded,selectedStatement=selected);a,b='173600000','173558000'
    source=dict(company_id=1,metric_id=2,metric_code='REVENUE',taxonomy='us-gaap',unit='USD',dimensions=None,cik='0000000001',period_start='2018-01-01',period_end='2018-12-31',form_type='10-K',source_tag='SalesRevenueNet',value=a,report_date='2018-12-31',filing_date='2019-02-01',accession_number='0000000001-18-000001',source_fact_hash='a'*64,source_url=original['url'],fiscal_year=2018)
    later=dict(source,source_tag='Revenues',value=b,report_date='2019-12-31',filing_date='2020-02-01',accession_number='0000000001-19-000001',source_fact_hash='b'*64,source_url=selected['url'])
    scope=[source,later]
    review=dict(companyId=1,metricId=2,metric='REVENUE',chosenTag='Revenues',start=source['period_start'],end=source['period_end'],reviewedOn='2026-10-31',sourceScopeHash=digest(scope),basis=basis,value=b,originalValue=a,filingDate=later['filing_date'],accessions=[later['accession_number']],sourceFactHashes=[later['source_fact_hash']],officialDocuments=[original['url'],selected['url']],originalBridgeEvidence=nodes,reviewExplanation='Original and later same-period statements independently confirm total; all source values and observations remain unchanged.')
    docs=[dict(url=original['url'],sha256=original['sha256']),dict(url=selected['url'],sha256=selected['sha256'])]
    return scope,dict(cik='0000000001',review=review,documents=docs)


@pytest.mark.parametrize('basis',[NESTED,PRECISE])
def test_registered_bridge_replays_without_changing_source_or_claiming_restatement(tmp_path,basis):
    rows,entry=fixture(tmp_path,basis);before=deepcopy(rows)
    a=validate_entry(rows,entry,tmp_path)
    assert rows==before and a['proof']['restatementClaimed'] is False
    assert a['proof_hash']==validate_entry(rows,entry,tmp_path)['proof_hash']
    assert 'revenueStandardAdjustment' not in a['proof']['review']
    if basis==NESTED:assert a['proof']['originalBridge']['detailsCountedOnce']
    else:assert a['proof']['originalBridge']['quantumUSD']=='100000'


@pytest.mark.parametrize('basis',[NESTED,PRECISE])
@pytest.mark.parametrize('change',['source_value','fact_hash','scope_hash','original_url','selected_url','fiscal_year','unit','period','table_hash'])
def test_identity_unit_period_or_source_change_fails_closed(tmp_path,basis,change):
    rows,entry=fixture(tmp_path,basis);e=entry['review']['originalBridgeEvidence']
    if change=='source_value':rows[0]['value']='123'
    elif change=='fact_hash':rows[1]['source_fact_hash']='c'*64
    elif change=='scope_hash':entry['review']['sourceScopeHash']='c'*64
    elif change=='original_url':e['originalStatement']['url']=e['selectedStatement']['url']
    elif change=='selected_url':e['selectedStatement']['url']=e['originalStatement']['url']
    elif change=='fiscal_year':e['originalStatement']['fiscalYear']=2017
    elif change=='unit':e['originalStatement']['unitScale']=1000000
    elif change=='period':entry['review']['end']='2018-12-30'
    elif change=='table_hash':e['originalStatement']['tableHash']='c'*64
    with pytest.raises(ValueError):validate_entry(rows,entry,tmp_path)


@pytest.mark.parametrize('change',['different_original_summary','precision','decimals','summary_row','summary_table','rounded_raw'])
def test_reverse_precision_requires_same_original_report_and_two_year_bridge(tmp_path,change):
    rows,entry=fixture(tmp_path,PRECISE);r=entry['review'];e=r['originalBridgeEvidence']
    if change=='different_original_summary':e['originalSummary']['url']=e['selectedStatement']['url']
    elif change=='precision':e['originalSummary']['unitScale']=1000
    elif change=='decimals':e['originalSummary']['decimalPlaces']=0
    elif change=='summary_row':e['originalSummary']['valueRow']=3
    elif change=='summary_table':e['originalSummary']['tableHash']=e['originalStatement']['tableHash']
    elif change=='rounded_raw':r['originalValue']='173500000'
    with pytest.raises(ValueError):validate_entry(rows,entry,tmp_path)


def test_nested_subtotal_cannot_be_added_twice(tmp_path):
    rows,entry=fixture(tmp_path);b=entry['review']['originalBridgeEvidence']['originalStatement']
    with pytest.raises(ValueError,match='BRIDGE_NESTED_SOURCE_VALUES'):
        nested(entry,tmp_path,b,'2018-12-31',Decimal(80000),Decimal(169000))


def test_summary_comparative_rounding_conflict_is_not_accepted(tmp_path):
    rows,entry=fixture(tmp_path,PRECISE);e=entry['review']['originalBridgeEvidence']
    first=detail(entry,tmp_path,e['originalStatement'],'2018-12-31',Decimal('173558000'));first['2017']=Decimal('176100000')
    with pytest.raises(ValueError,match='BRIDGE_ROUNDED_COMPARATIVE_CONFLICT'):
        rounded_summary(entry,tmp_path,e['originalSummary'],'2018-12-31',Decimal('173600000'),first)


def replace_original_document(entry, root, old_text, new_text):
    nodes=entry['review']['originalBridgeEvidence'];old=nodes['originalStatement'];sha=old['sha256'];url=old['url']
    body=gzip.decompress((root/'documents'/(sha+'.html.gz')).read_bytes()).decode()
    assert old_text in body
    changed=body.replace(old_text,new_text,1).encode();newsha=hashlib.sha256(changed).hexdigest()
    (root/'documents'/(newsha+'.html.gz')).write_bytes(gzip.compress(changed))
    p=Tables();p.feed(body);after=Tables();after.feed(changed.decode())
    for node in nodes.values():
        if node['url']==url:
            index=next(i for i,table in enumerate(p.tables) if digest(table)==node['tableHash'])
            node.update(sha256=newsha,tableHash=digest(after.tables[index]))
    next(d for d in entry['documents'] if d['url']==url)['sha256']=newsha


@pytest.mark.parametrize('old,new',[
    ('CONSOLIDATED STATEMENTS OF OPERATIONS','Business segment discussion'),
    ('for the years ended December 31,','for the three months ended December 31,'),
    ('in thousands','in millions'),
    ('Asset sales','Unrelated sales'),
    ('<td>20</td>','<td>21</td>'),
    ('<td>—</td>','<td>1</td>'),
    ('<td>80</td><td>80</td>','<td>80</td><td>81</td>'),
])
def test_nested_semantic_evidence_fails_even_with_new_valid_document_and_table_hashes(tmp_path,old,new):
    rows,entry=fixture(tmp_path);replace_original_document(entry,tmp_path,old,new)
    with pytest.raises(ValueError):validate_entry(rows,entry,tmp_path)


@pytest.mark.parametrize('old,new',[
    ('Consolidated net sales','Regional product sales'),
    ('<td>173.6</td>','<td>173.60</td>'),
    ('<td>176.7</td>','<td>176.6</td>'),
    ('<td>170.2</td>','<td>171.2</td>'),
    ('<td>176696</td>','<td>175696</td>'),
])
def test_precision_semantic_evidence_fails_even_with_new_valid_document_and_table_hashes(tmp_path,old,new):
    rows,entry=fixture(tmp_path,PRECISE);replace_original_document(entry,tmp_path,old,new)
    with pytest.raises(ValueError):validate_entry(rows,entry,tmp_path)
