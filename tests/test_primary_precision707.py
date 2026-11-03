import gzip
import hashlib
from copy import deepcopy
import pytest
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.primary_precision707 import BASIS, POLICY
from pipeline.financial_reconciliation677 import validate_entry


def fixture(root,mixed=False):
    (root/'documents').mkdir();docs=[];nodes=[];rows=[]
    data=[([2016,2015,2014],[1003066,900123,800111],1000,0),
          ([2017,2016,2015],['1123.4','1003.1','900.1'],1000000,1),
          ([2018,2017,2016],['1234.5','1123.4','1003.1'],1000000,1)]
    for i,(years,values,scale,places) in enumerate(data):
        mode='DATE' if mixed and i==1 else 'YEAR'
        cols=[f'December 31, {y}' for y in years] if mode=='DATE' else years
        tr=lambda cells:'<tr>'+''.join('<td>'+str(c)+'</td>' for c in cells)+'</tr>'
        body=('<p>Consolidated Statements of Income (in '+('thousands' if scale==1000 else 'millions')+')</p><table>'+tr(['Year ended December 31,',*cols])+tr(['Net sales',*values])+'</table>').encode()
        sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode())
        acc=f'0000000001-{17+i}-000001';url='https://www.sec.gov/Archives/edgar/data/1/'+acc.replace('-','')+'/annual.htm';docs.append(dict(url=url,sha256=sha));filed=f'{2017+i}-02-01'
        b=dict(url=url,sha256=sha,tableHash=digest(p.tables[0]),periodRow=0,valueRow=1,rowLabel='Net sales',unitScale=scale,periodMode=mode,annualHeading='Year ended December 31,')
        nodes.append(dict(statement=b,filingDate=filed,decimalPlaces=places))
        rows.append(dict(company_id=1,metric_id=2,metric_code='REVENUE',taxonomy='us-gaap',unit='USD',dimensions=None,cik='0000000001',period_start='2016-01-01',period_end='2016-12-31',form_type='10-K',source_tag='Revenues' if i==2 else 'SalesRevenueNet',value='1003066000' if i==0 else '1003100000',report_date=f'{2016+i}-12-31',filing_date=filed,accession_number=acc,source_fact_hash=chr(97+i)*64,source_url=url,fiscal_year=2016+i))
    r=dict(companyId=1,metricId=2,metric='REVENUE',chosenTag='Revenues',start='2016-01-01',end='2016-12-31',reviewedOn='2026-10-31',sourceScopeHash=digest(rows),basis=BASIS,value='1003100000',originalValue='1003066000',filingDate=rows[-1]['filing_date'],accessions=[rows[-1]['accession_number']],sourceFactHashes=[rows[-1]['source_fact_hash']],officialDocuments=[d['url'] for d in docs],primaryPrecisionEvidence=dict(nodes=nodes),reviewExplanation='All primary statements and stored source reports are bound; at least two actual annual periods overlap at each later printed precision. No unchanged-economics inference.')
    return rows,dict(cik='0000000001',review=r,documents=docs)


@pytest.mark.parametrize('mixed',[False,True])
def test_primary_chain_binds_every_report_and_actual_overlapping_period(tmp_path,mixed):
    rows,e=fixture(tmp_path,mixed);before=deepcopy(rows);a=validate_entry(rows,e,tmp_path)
    assert rows==before and a['proof']['policy']==POLICY
    assert a['proof']['quantumsUSD']==['1000','100000','100000']
    assert a['proof']['overlappingPeriodEnds']==[['2015-12-31','2016-12-31'],['2016-12-31','2017-12-31']]
    assert a['proof']['allSourceReportsBound'] and 'Not proof' in a['proof']['meaning']
    assert a['proof_hash']==validate_entry(rows,e,tmp_path)['proof_hash']


@pytest.mark.parametrize('change',['scope_hash','value','source_url','source_unit','source_period','source_form','node_date','node_url','table_hash','year_row','value_row','places','scale','period_mode','remove_middle','reverse','extra_report'])
def test_provenance_annual_dates_units_and_all_report_coverage_fail_closed(tmp_path,change):
    rows,e=fixture(tmp_path);r=e['review'];nodes=r['primaryPrecisionEvidence']['nodes'];n=nodes[1];b=n['statement']
    if change=='scope_hash':r['sourceScopeHash']='f'*64
    elif change in ['value','source_url','source_unit','source_period','source_form']:
        key,value={'value':('value','1003200000'),'source_url':('source_url',rows[0]['source_url']),'source_unit':('unit','EUR'),'source_period':('period_start','2016-10-01'),'source_form':('form_type','8-K')}[change];rows[1][key]=value;r['sourceScopeHash']=digest(rows)
    elif change=='node_date':n['filingDate']='2018-02-02'
    elif change=='node_url':b['url']=nodes[0]['statement']['url']
    elif change=='table_hash':b['tableHash']='f'*64
    elif change=='year_row':b['periodRow']=True
    elif change=='value_row':b['valueRow']=0
    elif change=='places':n['decimalPlaces']=2
    elif change=='scale':b['unitScale']=1000
    elif change=='period_mode':b['periodMode']='INFERRED_FISCAL_YEAR'
    elif change=='remove_middle':nodes.pop(1)
    elif change=='reverse':nodes.reverse()
    elif change=='extra_report':rows.append(dict(rows[1],source_url='https://www.sec.gov/Archives/edgar/data/1/000000000118000099/extra.htm'));r['sourceScopeHash']=digest(rows)
    with pytest.raises(ValueError):validate_entry(rows,e,tmp_path)


def rewrite(e,root,index,old,new):
    b=e['review']['primaryPrecisionEvidence']['nodes'][index]['statement'];sha=b['sha256'];body=gzip.decompress((root/'documents'/(sha+'.html.gz')).read_bytes()).replace(old.encode(),new.encode());newsha=hashlib.sha256(body).hexdigest();(root/'documents'/(newsha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());b.update(sha256=newsha,tableHash=digest(p.tables[0]));next(d for d in e['documents'] if d['url']==b['url'])['sha256']=newsha


@pytest.mark.parametrize('old,new',[
    ('900.1','900.2'),
    ('Year ended December 31,','Three months ended December 31,'),
    ('Consolidated Statements of Income (in millions)','Management discussion of consolidated statements of income and segment results (in millions)'),
    ('<td>2015</td>','<td>2013</td>'),
    ('Net sales','Product revenue component'),
    ('1003.1','1003.10'),
    ('<td>2017</td>','<td>2019</td>'),
    ('Year ended December 31,','Year ended December 31, and Nine months ended December 31,'),
])
def test_rehashed_documents_need_primary_title_every_comparative_value_and_printed_precision(tmp_path,old,new):
    rows,e=fixture(tmp_path);rewrite(e,tmp_path,1,old,new)
    with pytest.raises(ValueError):validate_entry(rows,e,tmp_path)


@pytest.mark.parametrize('context,cik,accepted',[
    ('Consolidated Statements of Earnings Years ended December 31, 2016, 2015 and 2014 (Dollar and share amounts in thousands, except per share data)','1',True),
    ('Consolidated Statements of Earnings Years ended December 31, 2016, 2014 and 2015 (Dollar and share amounts in thousands, except per share data)','1',False),
    ('Consolidated Statements of Operations Tennant Company and Subsidiaries (in thousands, except shares and per share data)','97134',True),
    ('Consolidated Statements of Operations Tennant Company and Subsidiaries (in thousands, except shares and per share data)','1',False),
    ('The following table is derived from consolidated statements of operations as a percentage of net sales.','1',False),
    ('Consolidated Statements of Earnings Segment Operating Results','1',False),
])
def test_explicit_title_layouts_require_company_and_calendar_column_binding(context,cik,accepted):
    from pipeline.primary_precision707 import primary_heading
    rows=[['2016','2015','2014']]
    if accepted:primary_heading({'cik':cik},rows,context,0)
    else:
        with pytest.raises(ValueError):primary_heading({'cik':cik},rows,context,0)


def test_primary_title_inside_leading_table_header_is_bound_before_years():
    from pipeline.primary_precision707 import primary_heading
    rows=[['Consolidated Statements of Income'],['2016','2015','2014']]
    primary_heading({'cik':'1'},rows,'Company annual financial statements',1)


def test_data_row_title_cannot_retroactively_bind_an_earlier_year_header():
    from pipeline.primary_precision707 import primary_heading
    with pytest.raises(ValueError):primary_heading({'cik':'1'},[['2016','2015','2014'],['Consolidated Statements of Income']],'Segment disclosures',0)
