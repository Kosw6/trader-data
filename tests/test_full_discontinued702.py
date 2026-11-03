import gzip
import hashlib
from copy import deepcopy
import pytest
from tests.test_discontinued_bridge687 import case
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.full_discontinued702 import BASIS,POLICY
from pipeline.financial_reconciliation677 import validate_entry


def fixture(tmp_path):
    rows,entry=case(tmp_path);r=entry['review'];e=r['discontinuedEvidence'];bindings={}
    for index,role in [(0,'original'),(1,'selected')]:
        d=entry['documents'][index];suffix=gzip.decompress((tmp_path/'documents'/(d['sha256']+'.html.gz')).read_bytes()) if index else b''
        years=[2023,2022] if index==0 else [2025,2024,2023]
        values=['0.100','0.080'] if index==0 else ['0.120','0.110','0.090']
        body=('<p>Consolidated Statements of Operations (in thousands)</p><table><tr><td>For the years ended June 30,</td>'+''.join('<td>'+str(y)+'</td>' for y in years)+'</tr><tr><td>Net sales</td>'+''.join('<td>'+v+'</td>' for v in values)+'</tr></table>').encode()+suffix
        sha=hashlib.sha256(body).hexdigest();(tmp_path/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));d['sha256']=sha;p=Tables();p.feed(body.decode())
        bindings[role]=dict(url=d['url'],sha256=sha,tableHash=digest(p.tables[0]),periodRow=0,valueRow=1,rowLabel='Net sales',unitScale=1000,periodMode='YEAR',annualHeading='For the years ended June 30,')
        if index:e.update(sha256=sha,tableHash=digest(p.tables[1]))
    r.update(basis=BASIS,fullStatementEvidence=bindings)
    return rows,entry


def test_original_and_selected_full_statements_are_replayed_with_existing_exact_cause(tmp_path):
    rows,entry=fixture(tmp_path);before=deepcopy(rows);a=validate_entry(rows,entry,tmp_path)
    assert rows==before and a['proof']['policy']==POLICY and a['proof']['review']['basis']==BASIS
    assert a['proof']['fullStatements']['original']['totalUSD']=='100.000'
    assert a['proof']['fullStatements']['selected']['totalUSD']=='90.000'
    assert not a['proof']['economicGrowthInferred'] and a['proof_hash']==validate_entry(rows,entry,tmp_path)['proof_hash']


@pytest.mark.parametrize('change',['original_url','selected_url','original_unit','original_label','selected_label','original_year','selected_table','cause','component_unit','removed','raw_value'])
def test_partial_or_unbound_statements_do_not_license_the_bridge(tmp_path,change):
    rows,entry=fixture(tmp_path);r=entry['review'];b=r['fullStatementEvidence'];e=r['discontinuedEvidence']
    if change=='original_url':b['original']['url']=b['selected']['url']
    elif change=='selected_url':b['selected']['url']=b['original']['url']
    elif change=='original_unit':b['original']['unitScale']=1000000
    elif change=='original_label':b['original']['rowLabel']='Segment sales'
    elif change=='selected_label':b['selected']['rowLabel']='Cost of sales'
    elif change=='original_year':b['original']['annualHeading']='For the years ended July 31,'
    elif change=='selected_table':b['selected']['tableHash']=e['tableHash']
    elif change=='cause':e['causeExcerpt']='A derivative was reclassified into income from discontinued operations for all periods presented.'
    elif change=='component_unit':e['unitScale']=1000000
    elif change=='removed':r['discontinuedRevenue']='9.9'
    elif change=='raw_value':rows[0]['value']='101';r['sourceScopeHash']=digest(rows)
    with pytest.raises(ValueError):validate_entry(rows,entry,tmp_path)
