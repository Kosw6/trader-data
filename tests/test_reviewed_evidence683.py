import gzip,hashlib
import pytest
from tests.test_precision_evidence682 import precision_case
from tests.test_financial_reconciliation677 import fixture
from pipeline.financial_reconciliation677 import validate_entry
from pipeline.reviewed_evidence683 import LAYOUT,ERROR
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.reconciliation_review679 import Tables

def replace_document(tmp_path,entry,index,body):
 sha=hashlib.sha256(body).hexdigest()
 with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
 entry['documents'][index]['sha256']=sha;p=Tables();p.feed(body.decode());return sha,digest(p.tables[0])

def layout(tmp_path):
 scope,entry,_=precision_case(tmp_path);r=entry['review'];r['basis']=LAYOUT
 body=b'<table><tr><td>(in thousands)</td></tr><tr><td>2023</td><td>2022</td><td>% Change</td></tr><tr><td>Net sales</td><td>105</td><td>90</td><td>16.7</td></tr></table>'
 sha,h=replace_document(tmp_path,entry,0,body);v=r['precisionEvidence']['originalRounded'];v.update(sha256=sha,tableHash=h,yearValueCount=2,percentValueCount=1);return scope,entry

def error(tmp_path):
 scope,entry,_=fixture(tmp_path);r=entry['review'];r['basis']=ERROR;r.pop('revenueStandardAdjustment')
 a='Product revenue was misstated for 2023.';b='Services revenue contained errors for 2023.'
 body=('<p>'+a+' '+b+'</p><table><tr><td>For the Year Ended June 30, 2023</td><td>(in USD)</td></tr><tr><td>As Previously Reported</td><td>Product Revenue</td><td>Services Revenue</td><td>As Restated</td></tr><tr><td>Net sales</td><td>100</td><td>3</td><td>7</td><td>110</td></tr></table>').encode()
 sha,h=replace_document(tmp_path,entry,1,body)
 r['errorComponentsEvidence']={'url':scope[1]['source_url'],'sha256':sha,'tableHash':h,'periodHeading':'For the Year Ended June 30, 2023','unitScale':1,'valueRow':2,'rowLabel':'Net sales','components':[{'label':'Product Revenue','amount':'3','causeExcerpt':a},{'label':'Services Revenue','amount':'7','causeExcerpt':b}]}
 return scope,entry

def test_percentage_columns_are_excluded_with_explicit_financial_year_prefix(tmp_path):
 scope,e=layout(tmp_path);a=validate_entry(scope,e,tmp_path)
 assert a['proof']['policy']=='sec-reviewed-percentage-layout-precision683-v1' and a['proof']['review']['basis']==LAYOUT

@pytest.mark.parametrize('key,value',[('yearValueCount',1),('yearValueCount',3),('percentValueCount',2),('fiscalYear',2022),('decimalPlaces',1),('tableHash','0'*64),('valueRow',1)])
def test_wrong_layout_never_approves(tmp_path,key,value):
 scope,e=layout(tmp_path);e['review']['precisionEvidence']['originalRounded'][key]=value
 with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)

def test_multi_component_error_is_not_revenue_standard_adoption(tmp_path):
 scope,e=error(tmp_path);a=validate_entry(scope,e,tmp_path)
 assert a['proof']['policy']=='sec-reviewed-multicomponent-error683-v1' and a['proof']['accountingCause']==ERROR

@pytest.mark.parametrize('case',['wrong_sum','wrong_cause','wrong_label','wrong_year','wrong_row','wrong_scale','missing_component','duplicate_component','changed_source'])
def test_incorrect_error_components_never_approve(tmp_path,case):
 scope,e=error(tmp_path);v=e['review']['errorComponentsEvidence']
 if case=='wrong_sum':v['components'][0]['amount']='4'
 if case=='wrong_cause':v['components'][0]['causeExcerpt']='Product revenue was fine.'
 if case=='wrong_label':v['components'][0]['label']='Inventories'
 if case=='wrong_year':v['periodHeading']='For the Year Ended June 30, 2022'
 if case=='wrong_row':v['valueRow']=1
 if case=='wrong_scale':v['unitScale']=1000
 if case=='missing_component':v['components'].pop()
 if case=='duplicate_component':v['components'][1]['label']=v['components'][0]['label']
 if case=='changed_source':scope.append(dict(scope[1],source_fact_hash='c'*64))
 with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)
