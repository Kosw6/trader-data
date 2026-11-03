import gzip,hashlib
from copy import deepcopy
from decimal import Decimal
import pytest
from pipeline.related_precision714 import CONTRACT,BASIS,POLICY,FOOTNOTE,vector,pension_footnote
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.financial_reconciliation677 import validate_entry


def fixture(root,index=1,old=None,new=None):
    (root/'documents').mkdir(exist_ok=True)
    data=[(['2017','2016','2015'],['1,198,076','1,178,631','1,867,711'],['391,004','140,463','82,146'],['1,589,080','1,319,094','1,949,857']),(['2018','2017*','2016*'],['1,204.5','1,198.1','1,178.6'],['688.7','391.0','140.5'],['1,893.2','1,589.1','1,319.1']),(['2019','2018','2017'],['1,191.6','1,204.5','1,198.1'],['645.0','688.7','391.0'],['1,836.6','1,893.2','1,589.1'])]
    years,a,b,c=data[index];unit='thousands' if index==0 else 'millions'
    rows=[[''],[''],['CENTURY ALUMINUM COMPANY'],['CONSOLIDATED STATEMENTS OF OPERATIONS'],[f'(in {unit}, except per share amounts)'],['','Year Ended December 31,'],['',*years],['NET SALES:'],['Related parties',*a],['Other customers',*b],['Total net sales',*c]]+[['Other statement line'] for _ in range(9)]
    body=('<table>'+''.join('<tr>'+''.join('<td>'+v+'</td>' for v in row)+'</tr>' for row in rows)+'</table>'+('<p>'+FOOTNOTE+'</p>' if index==1 else '')+'<p>CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME</p>').encode()
    if old is not None:body=body.replace(old.encode(),new.encode())
    sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());acc=f'0000949157-{18+index}-000001';d=dict(url='https://www.sec.gov/Archives/edgar/data/949157/'+acc.replace('-','')+'/annual.htm',sha256=sha)
    e=dict(cik='0000949157',documents=[d]);n=dict(statement=dict(**d,totalContract=CONTRACT,periodMode='EXACT_YEAR_STAR714',periodRow=6,valueRow=10,rowLabel='Total net sales',unitScale=1000 if index==0 else 1000000,tableHash=digest(p.tables[0])),filingDate=f'{2018+index}-02-28',decimalPlaces=0 if index==0 else 1)
    if index==1 and old is None:
        f=pension_footnote(n['statement'],root);n.update(footnoteHash=f['footnoteHash'],footnoteSectionHash=f['sectionHash'])
    return e,n,acc


def test_three_full_statement_component_vectors_with_source_bound_pension_note(tmp_path):
    for i,target in enumerate(['1589080000','1589100000','1589100000']):
        e,n,_=fixture(tmp_path,i);v,q=vector(e,tmp_path,n);assert v['2017-12-31']==Decimal(target) and q==(1000 if i==0 else 100000)


@pytest.mark.parametrize('old,new',[
    ('Related parties','Other customers'),('Other customers','Related parties'),('NET SALES:','SEGMENT SALES:'),('Total net sales','Sales subtotal'),
    ('1,198.1','1,198.2'),('140.5','140.6'),('1,893.2','1,893.3'),('688.7','688.7%'),
    ('2017*','2017'),('2016*','2015*'),('2017-07','2016-18'),('pension cost','revenue recognition'),
    ('except per share amounts','and millions'),('CONSOLIDATED STATEMENTS OF OPERATIONS','Unaudited Pro Forma Information'),
    ('CENTURY ALUMINUM COMPANY','Another company'),('Year Ended December 31,','Nine Months Ended December 31,'),
])
def test_rehashed_wrong_statement_components_footnotes_and_dates_fail_closed(tmp_path,old,new):
    e,n,_=fixture(tmp_path,old=old,new=new)
    # A source-changed note is not accepted merely by updating its binding hashes.
    try:
        f=pension_footnote(n['statement'],tmp_path);n.update(footnoteHash=f['footnoteHash'],footnoteSectionHash=f['sectionHash'])
    except ValueError:pass
    with pytest.raises(ValueError):vector(e,tmp_path,n)


def test_correct_footnote_in_another_section_does_not_satisfy_the_operations_note(tmp_path):
    e,n,_=fixture(tmp_path,old=FOOTNOTE,new='')
    b=n['statement'];p=tmp_path/'documents'/(b['sha256']+'.html.gz');body=gzip.decompress(p.read_bytes())+('<p>'+FOOTNOTE+'</p>').encode();sha=hashlib.sha256(body).hexdigest();(tmp_path/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));e['documents'][0]['sha256']=sha;b['sha256']=sha
    with pytest.raises(ValueError,match='FOOTNOTE'):pension_footnote(b,tmp_path)


@pytest.mark.parametrize('key,value',[('cik','1'),('totalContract','GENERIC'),('periodMode','YEAR'),('periodRow',5),('valueRow',8),('unitScale',1000),('rowLabel','Related parties'),('decimalPlaces',0),('filingDate','2017-01-01'),('footnoteHash','f'*64),('footnoteSectionHash','f'*64)])
def test_identity_precision_period_and_footnote_bindings_are_not_optional(tmp_path,key,value):
    e,n,_=fixture(tmp_path)
    if key=='cik':e[key]=value
    elif key in {'decimalPlaces','filingDate','footnoteHash','footnoteSectionHash'}:n[key]=value
    else:n['statement'][key]=value
    with pytest.raises(ValueError):vector(e,tmp_path,n)


def chain(root):
    nodes=[];documents=[];scope=[]
    for i in range(3):
        e,n,acc=fixture(root,i);nodes.append(n);documents+=e['documents'];scope.append(dict(company_id=1258,metric_id=22,metric_code='REVENUE',taxonomy='us-gaap',unit='USD',dimensions=None,cik='0000949157',period_start='2017-01-01',period_end='2017-12-31',form_type='10-K',source_tag='SalesRevenueNet' if i==0 else 'RevenueFromContractWithCustomerExcludingAssessedTax',value='1589080000' if i==0 else '1589100000',report_date=f'{2017+i}-12-31',filing_date=n['filingDate'],accession_number=acc,source_fact_hash=chr(97+i)*64,source_url=n['statement']['url'],fiscal_year=2017+i))
    r=dict(companyId=1258,metricId=22,metric='REVENUE',chosenTag=scope[-1]['source_tag'],start='2017-01-01',end='2017-12-31',reviewedOn='2026-10-31',sourceScopeHash=digest(scope),basis=BASIS,value='1589100000',originalValue='1589080000',filingDate=scope[-1]['filing_date'],accessions=[scope[-1]['accession_number']],sourceFactHashes=[scope[-1]['source_fact_hash']],officialDocuments=[d['url'] for d in documents],primaryPrecisionEvidence=dict(nodes=nodes),reviewExplanation='All related-party revenue components and full primary comparative amounts match at later printed precision. The exact operations-section pension-presentation footnote is bound. No income or other metric approval.')
    return scope,dict(cik='0000949157',review=r,documents=documents)


def test_complete_chain_verifies_every_raw_source_report_without_mutation(tmp_path):
    scope,e=chain(tmp_path);before=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
    assert before==scope and a['proof']['policy']==POLICY and a['proof']['quantumsUSD']==['1000','100000','100000']
    assert a['proof']['overlappingPeriodEnds']==[['2016-12-31','2017-12-31'],['2017-12-31','2018-12-31']]
    assert a['proof_hash']==validate_entry(scope,e,tmp_path)['proof_hash']


@pytest.mark.parametrize('change',['remove_middle','reverse','raw_value','source_date','source_unit','scope_hash','metric','company','latest_value'])
def test_chain_does_not_approve_partial_provenance_or_other_metrics(tmp_path,change):
    scope,e=chain(tmp_path);r=e['review'];nodes=r['primaryPrecisionEvidence']['nodes']
    if change=='remove_middle':nodes.pop(1)
    elif change=='reverse':nodes.reverse()
    elif change=='raw_value':scope[1]['value']='1589200000';r['sourceScopeHash']=digest(scope)
    elif change=='source_date':scope[1]['filing_date']='2019-03-01';r['sourceScopeHash']=digest(scope)
    elif change=='source_unit':scope[1]['unit']='EUR';r['sourceScopeHash']=digest(scope)
    elif change=='scope_hash':r['sourceScopeHash']='f'*64
    elif change=='metric':r['metric']='NET_INCOME'
    elif change=='company':r['companyId']=1
    else:r['value']='1589200000'
    with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)
