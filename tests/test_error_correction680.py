import gzip,hashlib,json
from copy import deepcopy
import pytest
from tests.test_financial_reconciliation677 import fixture,cursor
from pipeline import financial_reconciliation677 as module
from pipeline.error_correction680 import BASIS,POLICY
from pipeline.reviewed_financial_mapping672 import digest

def case(tmp_path,negative=False):
    scope,entry,manifest=fixture(tmp_path);r=entry['review'];r['basis']=BASIS
    if negative:
        scope[1]['value']='90';r['value']='90';r['sourceScopeHash']=digest(scope)
    delta='-10' if negative else '10';r['errorCorrectionAdjustment']=delta;r.pop('revenueStandardAdjustment')
    cause='(e) The correction of these misstatements increased net sales for 2023.'
    body=('<html><p>'+cause+'</p><table><tr><td>Year ended June 30, 2023</td></tr>'
      '<tr><td>(in millions)</td></tr><tr><td>As previously reported</td><td>Restatement impacts</td><td>As restated</td></tr>'
      '<tr><td>Net sales</td><td>100</td><td>'+('('+delta[1:]+')' if negative else delta)+'</td><td>(e)</td><td>'+r['value']+'</td></tr></table></html>').encode()
    sha=hashlib.sha256(body).hexdigest()
    # Fixture values are USD; this synthetic table is in single USD units.
    body=body.replace(b'(in millions)',b'(in USD)');sha=hashlib.sha256(body).hexdigest()
    with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
    entry['documents'][1]['sha256']=sha
    r['errorEvidence']={'category':BASIS,'causeExcerpt':cause,'reference':'(e)',
      'periodHeading':'Year ended June 30, 2023','periodEnd':r['end'],'metric':'REVENUE','rowLabel':'Net sales',
      'unitScale':1,'sourceUrl':scope[1]['source_url'],'documentSha256':sha,
      'priorAccession':scope[0]['accession_number'],'priorTag':scope[0]['source_tag']}
    manifest.write_text(json.dumps({'contract':module.CONTRACT,'entries':[entry]}))
    return scope,entry,manifest

@pytest.mark.parametrize('negative',[False,True])
def test_exact_error_correction_has_separate_policy_and_preserves_facts(tmp_path,negative):
    scope,entry,_=case(tmp_path,negative);before=deepcopy(scope)
    a=module.validate_entry(scope,entry,tmp_path)
    assert a['proof']['policy']==POLICY and a['proof']['review']['basis']==BASIS
    assert a['proof']['accountingCause']==BASIS and scope==before

@pytest.mark.parametrize('field,value',[
 ('causeExcerpt','(e) A different error correction.'),('periodHeading','Year ended June 30, 2022'),
 ('periodEnd','2022-06-30'),('metric','NET_INCOME'),('unitScale',1000),('rowLabel','Net income'),
 ('priorAccession','wrong'),('priorTag','wrong'),('reference','(f)'),('documentSha256','0'*64),
 ('category','FULL_RETROSPECTIVE_REVENUE_STANDARD')])
def test_incorrect_evidence_never_approves(tmp_path,field,value):
    scope,entry,_=case(tmp_path);entry['review']['errorEvidence'][field]=value
    with pytest.raises(ValueError):module.validate_entry(scope,entry,tmp_path)

def test_exact_adjustment_required(tmp_path):
    scope,entry,_=case(tmp_path);entry['review']['errorCorrectionAdjustment']='11'
    with pytest.raises(ValueError,match='RECONCILIATION'):module.validate_entry(scope,entry,tmp_path)

def test_prior_document_required(tmp_path):
    scope,entry,_=case(tmp_path);entry['review']['officialDocuments'].pop(0);entry['documents'].pop(0)
    with pytest.raises(ValueError,match='PRIOR_DOCUMENT'):module.validate_entry(scope,entry,tmp_path)

@pytest.mark.parametrize('existing',[False,True])
def test_worker_uses_error_policy_and_replay_is_idempotent(tmp_path,monkeypatch,existing):
    scope,entry,path=case(tmp_path);a=module.validate_entry(scope,entry,tmp_path)
    monkeypatch.setenv('FINANCIAL_RECONCILIATION_MANIFEST',str(path));monkeypatch.setattr(module,'discover_company',lambda *a:[])
    cur=cursor(scope,[(True,entry['review']['chosenTag'],a['proof_hash'],POLICY)] if existing else [])
    if existing:cur.fetchone.side_effect=[(77,)]
    result=module.apply_reviewed_company(cur,1,'2023-01-01','2023-12-31')
    assert result['results']=={('EXISTING' if existing else 'INSERTED'):1}
    inserts=[c for c in cur.execute.call_args_list if 'INSERT INTO finance_mapping667.approved' in c.args[0]]
    assert not inserts if existing else inserts[0].args[1][5]==POLICY

def test_existing_different_approval_is_never_overwritten(tmp_path,monkeypatch):
    scope,entry,path=case(tmp_path);monkeypatch.setenv('FINANCIAL_RECONCILIATION_MANIFEST',str(path));monkeypatch.setattr(module,'discover_company',lambda *a:[])
    cur=cursor(scope,[(True,'old','old-proof','old-policy')]);cur.fetchone.side_effect=[(77,)]
    result=module.apply_reviewed_company(cur,1,'2023-01-01','2023-12-31')
    assert result['excluded']=={'EXISTING_APPROVAL_PRESERVED':1}
    assert not any('INSERT INTO finance_mapping667.approved' in c.args[0] for c in cur.execute.call_args_list)
