import gzip
import hashlib
import json
from copy import deepcopy
from datetime import date
from unittest.mock import MagicMock
import pytest
from pipeline import financial_reconciliation677 as module
from pipeline.reviewed_financial_mapping672 import digest
from workers import financial_mapping_worker as worker
from workers.prepare_reconciliation677 import prepare

@pytest.fixture(autouse=True)
def separate_discovery_query_from_mutation_fixtures(monkeypatch):
    monkeypatch.setattr(module,'discover_company',lambda *args:[])

def fixture(tmp_path):
    row=dict(company_id=1,metric_id=2,metric_code='REVENUE',taxonomy='us-gaap',unit='USD',dimensions=None,
        cik='0000000001',period_start='2022-07-01',period_end='2023-06-30',form_type='10-K',
        source_tag='SalesRevenueNet',value='100',report_date='2023-06-30',filing_date='2023-08-01',
        accession_number='0000000001-23-000001',source_fact_hash='a'*64,
        source_url='https://www.sec.gov/Archives/edgar/data/1/000000000123000001/original.htm')
    later=dict(row,source_tag='RevenueFromContractWithCustomerExcludingAssessedTax',value='110',
        report_date='2025-06-30',filing_date='2025-08-01',accession_number='0000000001-25-000001',source_fact_hash='b'*64,
        source_url='https://www.sec.gov/Archives/edgar/data/1/000000000125000001/restated.htm')
    scope=[row,later]
    review=dict(companyId=1,metricId=2,metric='REVENUE',start=row['period_start'],end=row['period_end'],
        chosenTag=later['source_tag'],reviewedOn='2026-10-31',sourceScopeHash=digest(scope),
        basis='FULL_RETROSPECTIVE_REVENUE_STANDARD',value='110',originalValue='100',revenueStandardAdjustment='10',
        filingDate=later['filing_date'],accessions=[later['accession_number']],sourceFactHashes=['b'*64],
        officialDocuments=[row['source_url'],later['source_url']],reviewExplanation='Reviewed original 100 plus adjustment 10 = 110.')
    (tmp_path/'documents').mkdir();documents=[]
    for r in scope:
        body=('<html>Reviewed immutable filing '+r['source_url']+'</html>').encode();sha=hashlib.sha256(body).hexdigest()
        with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
        documents.append({'url':r['source_url'],'sha256':sha})
    entry={'cik':row['cik'],'review':review,'documents':documents}
    manifest=tmp_path/'review.json';manifest.write_text(json.dumps({'contract':module.CONTRACT,'entries':[entry]}))
    return scope,entry,manifest

def test_verified_bundle_replays_existing_strict_policy(tmp_path):
    scope,entry,manifest=fixture(tmp_path);before=deepcopy(scope)
    approved=module.validate_entry(scope,entry,tmp_path)
    assert approved['proof']['originalSources'][0]['value']=='100' and scope==before
    bundle,root=module.load_bundle(manifest);assert root==tmp_path and len(bundle['entries'])==1
    dest=tmp_path/'prepared';ready=prepare(manifest,dest)
    module.validate_entry(scope,module.load_bundle(ready)[0]['entries'][0],dest)

@pytest.mark.parametrize('case',['wrong_document','missing_original','other_company_url','non_sec','bad_sha','new_source','bad_adjustment','other_cik'])
def test_unreviewed_or_changed_evidence_never_approves(tmp_path,case):
    scope,entry,_=fixture(tmp_path)
    if case=='wrong_document':
        sha=entry['documents'][0]['sha256']
        with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(b'changed')
    if case=='missing_original':entry['documents'].pop(0)
    if case=='other_company_url':entry['documents'][0]['url']=entry['documents'][0]['url'].replace('/data/1/','/data/2/')
    if case=='non_sec':entry['documents'][0]['url']='http://169.254.169.254/latest'
    if case=='bad_sha':entry['documents'][0]['sha256']='../escape'
    if case=='new_source':scope.append(dict(scope[1],source_fact_hash='c'*64))
    if case=='bad_adjustment':entry['review']['revenueStandardAdjustment']='11'
    if case=='other_cik':entry['cik']='2'
    with pytest.raises((ValueError,FileNotFoundError)):module.validate_entry(scope,entry,tmp_path)

def test_duplicate_scope_manifest_rejected(tmp_path):
    _,entry,path=fixture(tmp_path);path.write_text(json.dumps({'contract':module.CONTRACT,'entries':[entry,entry]}))
    with pytest.raises(ValueError,match='DUPLICATE'):module.load_bundle(path)

def cursor(scope,existing):
    cur=MagicMock();cur.description=[(key,) for key in scope[0]]
    cur.fetchall.side_effect=[[tuple(r.values()) for r in scope],existing]
    cur.fetchone.side_effect=[None,(77,)]
    return cur

def test_atomic_apply_only_inserts_new_approved_scope_and_receipt(tmp_path,monkeypatch):
    scope,entry,path=fixture(tmp_path);monkeypatch.setenv('FINANCIAL_RECONCILIATION_MANIFEST',str(path))
    cur=cursor(scope,[]);result=module.apply_reviewed_company(cur,1,date(2023,1,1),date(2023,12,31),annual_only=True)
    assert result['results']=={'INSERTED':1} and result['receiptId']==77
    sql=' '.join(c.args[0] for c in cur.execute.call_args_list)
    assert 'INSERT INTO finance_mapping667.approved' in sql
    assert not any(c.args[0].lstrip().startswith(('UPDATE ','DELETE ','INSERT INTO public.')) for c in cur.execute.call_args_list)

def test_any_existing_approval_is_preserved(tmp_path,monkeypatch):
    scope,entry,path=fixture(tmp_path);monkeypatch.setenv('FINANCIAL_RECONCILIATION_MANIFEST',str(path))
    cur=cursor(scope,[(False,'old','old-proof','other-policy')]);cur.fetchone.side_effect=[(77,)]
    result=module.apply_reviewed_company(cur,1,date(2023,1,1),date(2023,12,31))
    assert result['excluded']=={'EXISTING_APPROVAL_PRESERVED':1}
    assert not any('INSERT INTO finance_mapping667.approved' in c.args[0] for c in cur.execute.call_args_list)

def test_idempotent_replay_does_not_insert_second_approval(tmp_path,monkeypatch):
    scope,entry,path=fixture(tmp_path);monkeypatch.setenv('FINANCIAL_RECONCILIATION_MANIFEST',str(path))
    approved=module.validate_entry(scope,entry,tmp_path)
    cur=cursor(scope,[(True,entry['review']['chosenTag'],approved['proof_hash'],module.POLICY)])
    cur.fetchone.side_effect=[(77,)]
    result=module.apply_reviewed_company(cur,1,date(2023,1,1),date(2023,12,31))
    assert result['results']=={'EXISTING':1}
    assert not any('INSERT INTO finance_mapping667.approved' in c.args[0] for c in cur.execute.call_args_list)

def test_new_filing_goes_to_review_without_replacing_history(tmp_path,monkeypatch):
    scope,entry,path=fixture(tmp_path);monkeypatch.setenv('FINANCIAL_RECONCILIATION_MANIFEST',str(path))
    scope.append(dict(scope[1],source_fact_hash='c'*64));cur=cursor(scope,[]);cur.fetchone.side_effect=[(77,)]
    result=module.apply_reviewed_company(cur,1,date(2023,1,1),date(2023,12,31))
    assert result['excluded']=={'RECONCILIATION_SOURCE_CHANGED':1}
    assert not any('INSERT INTO finance_mapping667.approved' in c.args[0] for c in cur.execute.call_args_list)

def test_unregistered_company_is_recorded_as_held_without_other_mapping(tmp_path,monkeypatch):
    _,_,path=fixture(tmp_path);monkeypatch.setenv('FINANCIAL_RECONCILIATION_MANIFEST',str(path));cur=MagicMock();cur.fetchone.return_value=(77,)
    result=module.apply_reviewed_company(cur,2,date(2023,1,1),date(2023,12,31))
    assert result['excluded']=={'RECONCILIATION_REVIEW_NOT_REGISTERED':1}

def test_worker_routes_special_mode_without_general_mapper(monkeypatch):
    connection=MagicMock();cur=connection.cursor.return_value.__enter__.return_value
    cur.fetchone.side_effect=[(1,4,date(2023,1,1),date(2023,12,31),True,False,module.REASON),(1,)]
    special=MagicMock(return_value={'companyId':1,'receiptId':77,'results':{'EXISTING':1}})
    normal=MagicMock(side_effect=AssertionError('normal mapping must not run'))
    monkeypatch.setattr(module,'apply_reviewed_company',special);monkeypatch.setattr(worker,'apply_company',normal)
    worker.process_one(connection);special.assert_called_once();normal.assert_not_called();connection.commit.assert_called_once()

def test_missing_configuration_rolls_back_without_queue_completion(monkeypatch):
    monkeypatch.delenv('FINANCIAL_RECONCILIATION_MANIFEST',raising=False)
    connection=MagicMock();cur=connection.cursor.return_value.__enter__.return_value
    cur.fetchone.return_value=(1,4,date(2023,1,1),date(2023,12,31),True,False,module.REASON)
    with pytest.raises(ValueError,match='NOT_CONFIGURED'):worker.process_one(connection)
    connection.commit.assert_not_called();connection.rollback.assert_called_once()
