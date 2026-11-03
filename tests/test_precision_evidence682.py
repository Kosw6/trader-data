import gzip,hashlib,json
from copy import deepcopy
import pytest
from tests.test_financial_reconciliation677 import fixture,cursor
from pipeline.financial_reconciliation677 import validate_entry
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.reconciliation_review679 import Tables
from pipeline.precision_evidence682 import BASIS,POLICY

def precision_case(tmp_path):
 scope,entry,path=fixture(tmp_path);scope[0].update(value='105499',fiscal_year=2023);scope[1].update(value='105000',fiscal_year=2025)
 r=entry['review'];r.update(basis=BASIS,originalValue='105499',value='105000',precisionQuantum='1000',sourceScopeHash=digest(scope));r.pop('revenueStandardAdjustment')
 body=b'<table><tr><td>(in thousands)</td></tr><tr><td>2023</td><td>2022</td></tr><tr><td>Net sales</td><td>105</td><td>90</td></tr></table>';sha=hashlib.sha256(body).hexdigest();parser=Tables();parser.feed(body.decode());tablehash=digest(parser.tables[0])
 with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
 for d in entry['documents']:d['sha256']=sha
 r['precisionEvidence']={role:{'url':scope[i]['source_url'],'sha256':sha,'tableHash':tablehash,'yearRow':1,'valueRow':2,'unitScale':1000,'decimalPlaces':0,'fiscalYear':2023} for i,role in enumerate(['originalRounded','latestRounded'])}
 path.write_text(json.dumps({'contract':'SEC_REVIEWED_RECONCILIATION677_V1','entries':[entry]}));return scope,entry,path

def test_same_original_report_already_shows_published_precision(tmp_path):
 scope,entry,_=precision_case(tmp_path);before=deepcopy(scope);a=validate_entry(scope,entry,tmp_path)
 assert a['proof']['policy']==POLICY and a['proof']['review']['basis']==BASIS and scope==before
 assert 'not proof of unchanged economics' in a['proof']['meaning']

@pytest.mark.parametrize('field,value',[('fiscalYear',2022),('tableHash','0'*64),('yearRow',0),('unitScale',1000000),('decimalPlaces',1),('url','https://www.sec.gov/Archives/edgar/data/1/000000000125000001/restated.htm')])
def test_wrong_original_bridge_never_approves(tmp_path,field,value):
 scope,entry,_=precision_case(tmp_path);entry['review']['precisionEvidence']['originalRounded'][field]=value
 with pytest.raises(ValueError):validate_entry(scope,entry,tmp_path)

def test_rounding_relationship_without_original_report_is_insufficient(tmp_path):
 scope,entry,_=precision_case(tmp_path);entry['review']['originalValue']='100000';scope[0]['value']='100000';entry['review']['sourceScopeHash']=digest(scope)
 with pytest.raises(ValueError,match='ROUNDING'):validate_entry(scope,entry,tmp_path)

def test_changed_raw_scope_is_rejected_even_with_same_html(tmp_path):
 scope,entry,_=precision_case(tmp_path);scope.append(dict(scope[1],source_fact_hash='c'*64))
 with pytest.raises(ValueError,match='SCOPE_CHANGED'):validate_entry(scope,entry,tmp_path)

@pytest.mark.parametrize('existing',[False,True])
def test_worker_records_precision_policy_and_replay_does_not_duplicate(tmp_path,monkeypatch,existing):
 from pipeline import financial_reconciliation677 as m
 scope,entry,path=precision_case(tmp_path);a=validate_entry(scope,entry,tmp_path)
 monkeypatch.setenv('FINANCIAL_RECONCILIATION_MANIFEST',str(path));monkeypatch.setattr(m,'discover_company',lambda *a:[])
 cur=cursor(scope,[(True,entry['review']['chosenTag'],a['proof_hash'],POLICY)] if existing else [])
 if existing:cur.fetchone.side_effect=[(77,)]
 result=m.apply_reviewed_company(cur,1,'2023-01-01','2023-12-31')
 assert result['results']=={('EXISTING' if existing else 'INSERTED'):1}
 inserts=[c for c in cur.execute.call_args_list if 'INSERT INTO finance_mapping667.approved' in c.args[0]]
 assert not inserts if existing else inserts[0].args[1][5]==POLICY
