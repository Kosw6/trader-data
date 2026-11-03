import gzip,hashlib
from decimal import Decimal
import pytest
from pipeline.property_services709 import statement,CONTRACT
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest

def fixture(root,old=None,new=None):
    rows=[[''],[''],['','Year Ended December 31,'],['','2016','2015','2014'],['REVENUES:'],['Property','5713126','4680388','4006854'],['Services','72542','91128','93194'],['Total operating revenues','5785668','4771516','4100048']]
    data=('<p>AMERICAN TOWER CORPORATION AND SUBSIDIARIES CONSOLIDATED STATEMENTS OF OPERATIONS (in thousands, except per share data)</p><table>'+''.join('<tr>'+''.join('<td>'+str(c)+'</td>' for c in r)+'</tr>' for r in rows)+'</table>').encode()
    if old:data=data.replace(old.encode(),new.encode())
    sha=hashlib.sha256(data).hexdigest();(root/'documents').mkdir();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(data));p=Tables();p.feed(data.decode());d=dict(url='https://www.sec.gov/Archives/edgar/data/1053507/000105350717000009/annual.htm',sha256=sha);e=dict(cik='1053507',documents=[d]);b=dict(**d,tableHash=digest(p.tables[0]),totalContract=CONTRACT,periodRow=3,valueRow=7,periodMode='YEAR',annualHeading='Year Ended December 31,',rowLabel='Total operating revenues',unitScale=1000)
    return e,b

def test_named_primary_total_and_all_three_comparative_sums(tmp_path):
    e,b=fixture(tmp_path);a=statement(e,tmp_path,b,'2016-12-31',Decimal('5785668000'));assert a['allThreeAnnualComponentEquationsVerified']

@pytest.mark.parametrize('old,new',[
    ('5713126','5713127'),('93194','93193'),('4771516','4771515'),('Property','Rent component'),('Services','Other income'),
    ('Total operating revenues','Segment revenue'),('REVENUES:','Segment revenues:'),('Year Ended December 31,','Three Months Ended December 31,'),
    ('<td>2015</td>','<td>2013</td>'),('in thousands','in millions'),('CONSOLIDATED STATEMENTS OF OPERATIONS','Selected segment results')])
def test_wrong_rehashed_document_still_fails_named_total_equations_and_annual_identity(tmp_path,old,new):
    e,b=fixture(tmp_path,old,new)
    with pytest.raises(ValueError):statement(e,tmp_path,b,'2016-12-31',Decimal('5785668000'))

@pytest.mark.parametrize('key,value',[('cik','1'),('totalContract','GENERIC'),('periodMode','INFERRED'),('periodRow',2),('valueRow',6),('unitScale',1000000),('rowLabel','Services')])
def test_contract_never_applies_as_a_global_revenue_alias(tmp_path,key,value):
    e,b=fixture(tmp_path)
    if key=='cik':e[key]=value
    else:b[key]=value
    with pytest.raises(ValueError):statement(e,tmp_path,b,'2016-12-31',Decimal('5785668000'))
