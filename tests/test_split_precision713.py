import gzip,hashlib
from decimal import Decimal
import pytest
from pipeline.split_precision713 import CONTRACT,vector
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest


def fixture(root,old=None,new=None,latest=False):
    rows=[['','','Fiscal Year Ended',''],['','','December 27,' if latest else 'December 29,','','','December 28,' if latest else 'December 30,','','','December 29,' if latest else 'December 25,',''],['','','2019' if latest else '2017','','','2018' if latest else '2016','','','2017' if latest else '2015',''],['(In millions, except per share amounts)' if latest else '(In thousands, except per share amounts)','']]
    if latest:
        rows += [['Revenues:','','',''],['Product','','840.8','1,015.5','924.4'],['Services','','225.4','81.0','-'],['Total revenues','','1,066.2','1,096.5','924.4']]
    else:rows += [['Sales','','924,351','562,759','469,103']]
    rows += [['Other statement line',''] for _ in range(14-len(rows))]
    body=('<p>Ultra Clean Holdings, Inc. Consolidated Statements of Operations</p><table>'+''.join('<tr>'+''.join('<td>'+c+'</td>' for c in row)+'</tr>' for row in rows)+'</table>').encode()
    if old is not None:body=body.replace(old.encode(),new.encode())
    sha=hashlib.sha256(body).hexdigest();(root/'documents').mkdir();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body))
    parser=Tables();parser.feed(body.decode());doc=dict(url='https://www.sec.gov/Archives/edgar/data/1275014/000156459020010037/annual.htm',sha256=sha)
    entry=dict(cik='0001275014',documents=[doc]);e=dict(**doc,totalContract=CONTRACT,periodMode='SPLIT_MONTH_DAY_YEAR713',periodRow=2,monthDayRow=1,unitRow=3,valueRow=7 if latest else 4,rowLabel='Total revenues' if latest else 'Sales',unitScale=1000000 if latest else 1000,tableHash=digest(parser.tables[0]))
    return entry,dict(statement=e,filingDate='2020-03-11' if latest else '2018-03-14',decimalPlaces=1 if latest else 0)


def test_actual_fiscal_dates_are_paired_in_the_same_columns_not_calendar_year_ends(tmp_path):
    e,n=fixture(tmp_path);v,q=vector(e,tmp_path,n)
    assert v=={'2017-12-29':Decimal('924351000'),'2016-12-30':Decimal('562759000'),'2015-12-25':Decimal('469103000')} and q==1000


def test_later_statement_checks_all_product_service_totals_at_its_printed_precision(tmp_path):
    e,n=fixture(tmp_path,latest=True);v,q=vector(e,tmp_path,n)
    assert v=={'2019-12-27':Decimal('1066200000'),'2018-12-28':Decimal('1096500000'),'2017-12-29':Decimal('924400000')} and q==100000


@pytest.mark.parametrize('old,new',[
    ('Fiscal Year Ended','Three Months Ended'),('Consolidated Statements of Operations','Selected segment results'),
    ('Consolidated Statements of Operations','Unaudited Pro Forma Information'),('Sales','Segment sales'),
    ('<td>2016</td>','<td></td><td>2016</td>'),('2016','2017'),('December 25,','June 25,'),
    ('(In thousands, except per share amounts)','(In thousands and millions, except per share amounts)'),
    ('924,351','924,351%'),('562,759','562,759.0'),('469,103','469,103</td><td>0'),
])
def test_rehashed_header_unit_scope_and_value_changes_are_rejected(tmp_path,old,new):
    e,n=fixture(tmp_path,old,new)
    with pytest.raises(ValueError):vector(e,tmp_path,n)


@pytest.mark.parametrize('old,new',[
    ('225.4','225.5'),('81.0','81.1'),('924.4','924.5'),('Services','Other income'),('Product','Segments'),
    ('Revenues:','Segment revenues:'),('<td>-</td>','<td></td>'),('1,066.2','1,066.3'),
])
def test_all_comparative_component_equations_and_hierarchy_are_required(tmp_path,old,new):
    e,n=fixture(tmp_path,old,new,latest=True)
    # Changing every occurrence of 924.4 preserves one equation but changes the
    # target amount: comparison with the original/raw chain must then fail.
    if old=='924.4':
        v,_=vector(e,tmp_path,n);assert v['2017-12-29']!=Decimal('924400000');return
    with pytest.raises(ValueError):vector(e,tmp_path,n)


@pytest.mark.parametrize('key,value',[('cik','1'),('totalContract','GENERIC'),('periodRow',1),('monthDayRow',2),('unitRow',0),('valueRow',5),('periodMode','YEAR'),('unitScale',1000000),('rowLabel','Other sales'),('decimalPlaces',1),('filingDate','2016-01-01')])
def test_contract_metadata_cannot_bypass_identity_dates_units_or_values(tmp_path,key,value):
    e,n=fixture(tmp_path)
    if key=='cik':e[key]=value
    elif key in {'decimalPlaces','filingDate'}:n[key]=value
    else:n['statement'][key]=value
    with pytest.raises(ValueError):vector(e,tmp_path,n)


def test_source_bytes_are_rehashed_even_if_parsing_was_cached(tmp_path):
    e,n=fixture(tmp_path);vector(e,tmp_path,n)
    p=tmp_path/'documents'/(n['statement']['sha256']+'.html.gz');p.write_bytes(gzip.compress(b'changed'))
    with pytest.raises(ValueError,match='DOCUMENT_CHANGED'):vector(e,tmp_path,n)


def chain(root):
    from pipeline.split_precision713 import BASIS
    (root/'documents').mkdir();nodes=[];docs=[];scope=[]
    for i in range(3):
        sub=root/str(i);sub.mkdir();e,n=fixture(sub,latest=i==2)
        body=gzip.decompress((sub/'documents'/(n['statement']['sha256']+'.html.gz')).read_bytes())
        if i==1:
            for old,new in [('2017','A_YEAR'),('2016','B_YEAR'),('2015','C_YEAR'),('A_YEAR','2018'),('B_YEAR','2017'),('C_YEAR','2016'),('December 29,','A_DAY'),('December 30,','B_DAY'),('December 25,','C_DAY'),('A_DAY','December 28,'),('B_DAY','December 29,'),('C_DAY','December 30,'),('924,351','A_VAL'),('562,759','B_VAL'),('469,103','C_VAL'),('A_VAL','1,096,545'),('B_VAL','924,351'),('C_VAL','562,759')]:body=body.replace(old.encode(),new.encode())
        sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode())
        acc=f'0001275014-{18+i}-000001';url='https://www.sec.gov/Archives/edgar/data/1275014/'+acc.replace('-','')+'/annual.htm';filed=['2018-03-14','2019-03-22','2020-03-11'][i]
        doc=dict(url=url,sha256=sha);docs.append(doc);n['statement'].update(**doc,tableHash=digest(p.tables[0]));n['filingDate']=filed;nodes.append(n)
        scope.append(dict(company_id=1942,metric_id=22,metric_code='REVENUE',taxonomy='us-gaap',unit='USD',dimensions=None,cik='0001275014',period_start='2016-12-31',period_end='2017-12-29',form_type='10-K',source_tag='RevenueFromContractWithCustomerExcludingAssessedTax' if i else 'SalesRevenueNet',value='924400000' if i==2 else '924351000',report_date=['2017-12-29','2018-12-28','2019-12-27'][i],filing_date=filed,accession_number=acc,source_fact_hash=chr(97+i)*64,source_url=url,fiscal_year=2017+i))
    review=dict(companyId=1942,metricId=22,metric='REVENUE',chosenTag=scope[-1]['source_tag'],start='2016-12-31',end='2017-12-29',reviewedOn='2026-10-31',sourceScopeHash=digest(scope),basis=BASIS,value='924400000',originalValue='924351000',filingDate=scope[-1]['filing_date'],accessions=[scope[-1]['accession_number']],sourceFactHashes=[scope[-1]['source_fact_hash']],officialDocuments=[d['url'] for d in docs],primaryPrecisionEvidence=dict(nodes=nodes),reviewExplanation='Three exact full consolidated statements, varying fiscal dates, all comparative columns and every raw source report bound; no inference below displayed precision.')
    return scope,dict(cik='0001275014',review=review,documents=docs)


def test_complete_chain_binds_original_middle_and_latest_raw_without_mutation(tmp_path):
    from copy import deepcopy
    from pipeline.financial_reconciliation677 import validate_entry
    from pipeline.split_precision713 import POLICY
    scope,e=chain(tmp_path);before=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
    assert scope==before and a['proof']['policy']==POLICY and a['proof']['quantumsUSD']==['1000','1000','100000']
    assert a['proof']['overlappingPeriodEnds']==[['2016-12-30','2017-12-29'],['2017-12-29','2018-12-28']]
    assert a['proof_hash']==validate_entry(scope,e,tmp_path)['proof_hash']


@pytest.mark.parametrize('change',['remove_middle','reverse','raw_value','source_url','source_date','source_start','source_form','source_unit','scope_hash','wrong_company','wrong_metric','wrong_review_end','false_original','false_latest'])
def test_complete_chain_rejects_missing_or_changed_provenance(tmp_path,change):
    from pipeline.financial_reconciliation677 import validate_entry
    scope,e=chain(tmp_path);r=e['review'];nodes=r['primaryPrecisionEvidence']['nodes']
    if change=='remove_middle':nodes.pop(1)
    elif change=='reverse':nodes.reverse()
    elif change=='scope_hash':r['sourceScopeHash']='f'*64
    elif change=='wrong_company':r['companyId']=1
    elif change=='wrong_metric':r['metric']='NET_INCOME'
    elif change=='wrong_review_end':r['end']='2017-12-31'
    elif change=='false_original':r['originalValue']='924350000'
    elif change=='false_latest':r['value']='924500000'
    else:
        k,v={'raw_value':('value','924350000'),'source_url':('source_url',scope[0]['source_url']),'source_date':('filing_date','2019-03-23'),'source_start':('period_start','2017-10-01'),'source_form':('form_type','8-K'),'source_unit':('unit','EUR')}[change];scope[1][k]=v;r['sourceScopeHash']=digest(scope)
    with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)
