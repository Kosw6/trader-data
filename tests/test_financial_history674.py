from copy import deepcopy
from datetime import date
from decimal import Decimal
from unittest.mock import Mock
import pytest
from collectors.common import content_hash
from pipeline.financial_history674 import raw_index,unchanged_approval,scope_hash,capture_collected_evidence,store_evidence

ASOF=date(2026,10,30)
SOURCE={'canonicalJsonSha256':'d'*64,'objectUri':'s3://approved-sec-archive/company.json'}
CONTRACT='RevenueFromContractWithCustomerExcludingAssessedTax'

def example(code='REVENUE',original_tag='SalesRevenueNet'):
    from pipeline.financial_mapping667 import RULES
    unit,tags=RULES[code];tag=CONTRACT if code=='REVENUE' else sorted(tags)[0]
    if code!='REVENUE':original_tag=tag
    payload={'cik':1045810,'facts':{'us-gaap':{}}};rows=[]
    for i,(concept,year) in enumerate([(original_tag,2025),(tag,2026)]):
        obs={'start':'2024-01-01','end':'2024-12-31','val':100,'accn':f'0001045810-{str(year)[2:]}-000001','filed':f'{year}-02-01','form':'10-K','fy':year-1,'fp':'FY'}
        if code in {'ASSETS','LIABILITIES'}:obs.pop('start')
        payload['facts']['us-gaap'].setdefault(concept,{'units':{}})['units'].setdefault(unit,[]).append(obs)
        h=content_hash({'cik':'0001045810','taxonomy':'us-gaap','source_tag':concept,'unit':unit,'fact':obs})
        rows.append({'fact_id':i+1,'company_id':1,'metric_id':2,'metric_code':code,'taxonomy':'us-gaap','source_tag':concept,'unit':unit,
            'period_start':obs.get('start'),'period_end':obs['end'],'value':Decimal('100.0000000000'),'dimensions':None,'source_fact_hash':h,
            'accession_number':obs['accn'],'filing_date':obs['filed'],'report_date':f'{year-1}-12-31','form_type':'10-K','cik':'0001045810',
            'source_url':'https://www.sec.gov/Archives/edgar/data/1045810/'+obs['accn'].replace('-','')+'/annual.htm'})
    return rows,payload

@pytest.mark.parametrize('code',['REVENUE','NET_INCOME','OPERATING_INCOME','ASSETS','LIABILITIES','DILUTED_EPS'])
def test_all_six_metrics_only_from_matching_raw_unchanged_originals(code):
    rows,payload=example(code);d=unchanged_approval(rows,raw_index(payload,1045810),SOURCE,today=ASOF)
    assert d['outcome']=='APPROVE' and d['source_tag']!='SalesRevenueNet'
    assert d['proof']['basis']=='UNCHANGED_ORIGINAL_AND_COMPARATIVE_SEC_XBRL'

@pytest.mark.parametrize('change,reason',[
    ({'value':Decimal('101')},'RAW_OBSERVATION_MISMATCH'),
    ({'source_fact_hash':'e'*64},'RAW_FACT_NOT_FOUND'),
    ({'dimensions':{'segment':'cloud'}},'UNREVIEWED_UNIT_TAXONOMY_OR_DIMENSIONS'),
    ({'unit':'EUR'},'UNREVIEWED_UNIT_TAXONOMY_OR_DIMENSIONS'),
    ({'taxonomy':'ifrs-full'},'UNREVIEWED_UNIT_TAXONOMY_OR_DIMENSIONS'),
    ({'source_tag':'RevenueFromContractWithCustomerIncludingAssessedTax'},'UNREVIEWED_TAG'),
    ({'source_url':'https://www.sec.gov.evil.invalid/a'},'INVALID_SOURCE_URL'),
    ({'company_id':2},'MIXED_SOURCE_IDENTITY'),
    ({'filing_date':'2027-01-01'},'INVALID_VALUE_OR_DATE'),
])
def test_untrusted_metadata_or_missing_raw_never_approves(change,reason):
    rows,payload=example();rows[-1].update(change)
    with pytest.raises(ValueError,match=reason):unchanged_approval(rows,raw_index(payload,1045810),SOURCE,today=ASOF)

def test_even_raw_verified_changed_totals_require_manual_reconciliation():
    rows,payload=example();obs=payload['facts']['us-gaap'][CONTRACT]['units']['USD'][0];obs['val']=101
    rows[-1]['value']=Decimal('101');rows[-1]['source_fact_hash']=content_hash({'cik':'0001045810','taxonomy':'us-gaap','source_tag':CONTRACT,'unit':'USD','fact':obs})
    with pytest.raises(ValueError,match='RESTATEMENT_REVIEW_REQUIRED'):unchanged_approval(rows,raw_index(payload,1045810),SOURCE,today=ASOF)

def test_comparatives_without_original_report_are_not_enough():
    rows,payload=example();rows[0]['report_date']='2025-12-31'
    with pytest.raises(ValueError,match='ORIGINAL_REPORT_NOT_FOUND'):unchanged_approval(rows,raw_index(payload,1045810),SOURCE,today=ASOF)

def test_company_identity_is_bound_to_raw_payload():
    _,payload=example()
    with pytest.raises(ValueError,match='RAW_COMPANY_IDENTITY_MISMATCH'):raw_index(payload,42)

def test_proof_is_stable_across_storage_types_and_detects_source_changes():
    rows,_=example();h=scope_hash(rows);copy=deepcopy(rows)
    for r in copy:r.update(fact_id=str(r['fact_id']),period_end=date.fromisoformat(r['period_end']),value='100')
    assert scope_hash(copy)==h
    copy[0]['value']='101';assert scope_hash(copy)!=h

def test_evidence_is_opt_in_and_no_raw_means_no_database_write(monkeypatch):
    cur=Mock();monkeypatch.delenv('FINANCIAL_HISTORY_RAW_ENABLED',raising=False)
    capture_collected_evidence(cur,{},{});cur.execute.assert_not_called()
    monkeypatch.setenv('FINANCIAL_HISTORY_RAW_ENABLED','true')
    capture_collected_evidence(cur,{},{});cur.execute.assert_not_called()

def test_evidence_rejects_cross_company_or_corrupt_prepared_proof():
    rows,payload=example();d=unchanged_approval(rows,raw_index(payload,1045810),SOURCE,today=ASOF)
    with pytest.raises(ValueError,match='INVALID_PREPARED_EVIDENCE'):store_evidence(Mock(),2,[d])
    d['proof']['tag']='Other'
    with pytest.raises(ValueError,match='INVALID_PREPARED_EVIDENCE'):store_evidence(Mock(),1,[d])

def test_hash_never_rounds_sql_numeric_precision():
    rows,_=example();rows[0]['value']='1234567890123456789012345678.0000000001';h=scope_hash(rows)
    rows[0]['value']='1234567890123456789012345678.0000000002'
    assert scope_hash(rows)!=h

@pytest.mark.parametrize('change',['none','source_changed','global_mapping','reviewed_mapping','inactive_approval'])
def test_repair_mode_only_applies_current_raw_evidence_and_preserves_existing_history(monkeypatch,change):
    from pipeline import financial_history674 as history
    rows,payload=example();d=unchanged_approval(rows,raw_index(payload,1045810),SOURCE,today=ASOF)
    monkeypatch.setenv('FINANCIAL_HISTORY_RAW_ENABLED','true')
    monkeypatch.setattr(history,'read_rows',lambda *args:rows)
    cur=Mock();cur.fetchone.return_value=({'proposals674':[d]},)
    prior=[];mappings=[]
    if change=='source_changed':rows[0]['value']='101'
    if change=='global_mapping':mappings=[(2,None,'us-gaap','Revenues',None,None)]
    if change in {'reviewed_mapping','inactive_approval'}:
        prior=[(2,date(2024,12,31),'Revenues',change=='reviewed_mapping','reviewed-old-policy',{},'e'*64)]
    cur.fetchall.side_effect=[prior,mappings]
    normal=[dict(d,source_tag='Unverified',outcome='APPROVE')]
    result=history.merge_verified_history(cur,1,date(2024,1,1),ASOF,False,normal,repair_only=True)
    assert len(result)==(1 if change=='none' else 0)
    assert not any('UPDATE' in c.args[0] or 'INSERT' in c.args[0] for c in cur.execute.call_args_list)

def test_repair_job_cannot_fall_back_to_unverified_primary_rules_when_feature_is_disabled(monkeypatch):
    from pipeline.financial_history674 import merge_verified_history
    monkeypatch.delenv('FINANCIAL_HISTORY_RAW_ENABLED',raising=False)
    with pytest.raises(ValueError,match='RAW_HISTORY_FEATURE_DISABLED'):
        merge_verified_history(Mock(),1,date(2024,1,1),ASOF,False,[{'outcome':'APPROVE'}],repair_only=True)

def test_evidence_outer_fields_cannot_change_the_reviewed_target():
    rows,payload=example();d=unchanged_approval(rows,raw_index(payload,1045810),SOURCE,today=ASOF)
    d['source_tag']='SalesRevenueNet'
    with pytest.raises(ValueError,match='INVALID_PREPARED_EVIDENCE'):store_evidence(Mock(),1,[d])

def test_etl_queue_and_raw_proof_share_commit_and_mapper_lock_order(monkeypatch):
    from collections import Counter
    from unittest.mock import MagicMock
    from backfills.sec_financial import backfill
    connection=MagicMock();events=[]
    for name in ['seed_financial_metrics','upsert_filings','upsert_financial_facts']:
        monkeypatch.setattr(backfill,name,lambda *a,**kw:{})
    monkeypatch.setattr('pipeline.financial_mapping667.enqueue_if_enabled',lambda *a:events.append('queue'))
    def proof(*args):
        connection.commit.assert_not_called();events.append('raw_proof')
    monkeypatch.setattr('pipeline.financial_history674.capture_collected_evidence',proof)
    backfill.apply_company_financials(connection,{'company_id':1},{'source_object':[],'filing':[],'financial_fact':[]},None,Counter(),[])
    assert events==['queue','raw_proof'];connection.commit.assert_called_once()

@pytest.mark.parametrize('fresh',[True,False])
def test_existing_auto_history_rechecks_new_sources_and_holds_unverified_changes(monkeypatch,fresh):
    from pipeline import financial_history674 as history
    rows,payload=example();old=unchanged_approval(rows,raw_index(payload,1045810),SOURCE,today=ASOF)
    # A new immutable observation identity changes the scope fingerprint.
    rows[0]['fact_id']=99
    new=unchanged_approval(rows,raw_index(payload,1045810),SOURCE,today=ASOF)
    monkeypatch.setenv('FINANCIAL_HISTORY_RAW_ENABLED','true');monkeypatch.setattr(history,'read_rows',lambda *a:rows)
    cur=Mock();cur.fetchone.return_value=({'proposals674':[new] if fresh else []},)
    cur.fetchall.side_effect=[[(2,date(2024,12,31),old['source_tag'],True,history.POLICY,old['proof'],old['proof_hash'])],
                             [(2,1,'us-gaap',old['source_tag'],date(2024,12,31),date(2024,12,31))]]
    result=history.merge_verified_history(cur,1,date(2024,1,1),ASOF,False,[])
    assert len(result)==1 and result[0]['outcome']==('EXISTING' if fresh else 'HELD')
    if not fresh:assert result[0]['reason']=='RAW_SOURCE_CHANGED_REVIEW_REQUIRED'

@pytest.mark.parametrize('existing',['none','our_verified_policy','legacy_review'])
def test_future_collection_raw_payload_really_reaches_the_evidence_recorder(monkeypatch,existing):
    from pipeline import financial_history674 as history
    rows,payload=example();original=deepcopy(payload)
    monkeypatch.setenv('FINANCIAL_HISTORY_RAW_ENABLED','true');monkeypatch.setattr(history,'read_rows',lambda *a:rows)
    cur=Mock();mappings=[] if existing=='none' else [(2,date(2024,12,31),date(2024,12,31))]
    refreshable=[(2,date(2024,12,31))] if existing=='our_verified_policy' else []
    cur.fetchall.side_effect=[mappings,refreshable];recorder=Mock();monkeypatch.setattr(history,'store_evidence',recorder)
    records={'raw_payloads674':[({'content_hash':content_hash(payload),'object_uri':SOURCE['objectUri']},payload)]}
    history.capture_collected_evidence(cur,{'company_id':1,'cik':'0001045810'},records)
    plans=recorder.call_args.args[2]
    assert len(plans)==(0 if existing=='legacy_review' else 1)
    assert payload==original
    if plans:assert plans[0]['source_tag']==CONTRACT and plans[0]['proof']['rawSource']['canonicalJsonSha256']==content_hash(payload)

def test_corrupted_future_raw_object_cannot_record_evidence(monkeypatch):
    from pipeline import financial_history674 as history
    rows,payload=example();monkeypatch.setenv('FINANCIAL_HISTORY_RAW_ENABLED','true')
    monkeypatch.setattr(history,'read_rows',lambda *a:rows);recorder=Mock();monkeypatch.setattr(history,'store_evidence',recorder)
    cur=Mock();cur.fetchall.side_effect=[[],[]]
    with pytest.raises(ValueError,match='RAW_CONTENT_HASH_CHANGED'):
        history.capture_collected_evidence(cur,{'company_id':1,'cik':'0001045810'},
            {'raw_payloads674':[({'content_hash':'e'*64,'object_uri':SOURCE['objectUri']},payload)]})
    recorder.assert_not_called()

def test_kafka_etl_preserves_verified_raw_payload_until_the_transactional_hook(monkeypatch):
    from types import SimpleNamespace
    from workers import sec_financial_etl_worker as etl
    _,payload=example();submissions={'cik':'0001045810','filings':{'recent':{}}}
    sources=[{'id':1,'data_type':'SUBMISSIONS','object_uri':'s3://raw/submissions','parameters':{}},
             {'id':2,'data_type':'COMPANY_FACTS','object_uri':SOURCE['objectUri'],'content_hash':content_hash(payload)}]
    monkeypatch.setattr(etl,'read_raw_payload',lambda s:submissions if s['id']==1 else payload)
    monkeypatch.setattr(etl,'source_preview',lambda s:s)
    monkeypatch.setattr(etl,'transform_filings',lambda *a:[])
    monkeypatch.setattr(etl,'transform_financial_facts',lambda *a,**kw:[{'source_tag':'kept'}])
    records=etl.records_from_sec_sources(sources,SimpleNamespace(all_facts=False))
    assert records['financial_fact']==[{'source_tag':'kept'}]
    assert records['raw_payloads674']==[(sources[1],payload)]
    assert records['raw_payloads674'][0][1] is payload

@pytest.mark.parametrize('form,allowed',[('10-K',True),('10-Q',False)])
def test_annual_go_scope_requires_original_annual_report_even_for_instant_metrics(monkeypatch,form,allowed):
    from pipeline import financial_history674 as history
    rows,payload=example('ASSETS')
    if form=='10-Q':
        observations=payload['facts']['us-gaap']['Assets']['units']['USD']
        for row,obs in zip(rows,observations):
            obs['form']=form;row['form_type']=form
            row['source_fact_hash']=content_hash({'cik':'0001045810','taxonomy':'us-gaap','source_tag':'Assets','unit':'USD','fact':obs})
    d=unchanged_approval(rows,raw_index(payload,1045810),SOURCE,today=ASOF)
    monkeypatch.setenv('FINANCIAL_HISTORY_RAW_ENABLED','true');monkeypatch.setattr(history,'read_rows',lambda *a:rows)
    cur=Mock();cur.fetchone.return_value=({'proposals674':[d]},);cur.fetchall.side_effect=[[],[]]
    result=history.merge_verified_history(cur,1,date(2024,1,1),ASOF,True,[],repair_only=True)
    assert len(result)==int(allowed)

def test_annual_year_end_can_keep_corroborated_q4_facts_under_the_same_tag(monkeypatch):
    from pipeline import financial_history674 as history
    rows,payload=example()
    for i,(tag,year) in enumerate([('SalesRevenueNet',2025),(CONTRACT,2026)]):
        obs={'start':'2024-10-01','end':'2024-12-31','val':25,'accn':f'0001045810-{str(year)[2:]}-000001','filed':f'{year}-02-01','form':'10-K','fy':year-1,'fp':'FY'}
        payload['facts']['us-gaap'][tag]['units']['USD'].append(obs)
        row={**rows[i],'fact_id':i+10,'period_start':obs['start'],'value':'25'}
        row['source_fact_hash']=content_hash({'cik':'0001045810','taxonomy':'us-gaap','source_tag':tag,'unit':'USD','fact':obs});rows.append(row)
    d=unchanged_approval(rows,raw_index(payload,1045810),SOURCE,today=ASOF)
    monkeypatch.setenv('FINANCIAL_HISTORY_RAW_ENABLED','true');monkeypatch.setattr(history,'read_rows',lambda *a:rows)
    cur=Mock();cur.fetchone.return_value=({'proposals674':[d]},);cur.fetchall.side_effect=[[],[]]
    result=history.merge_verified_history(cur,1,date(2024,1,1),ASOF,True,[],repair_only=True)
    assert len(result)==1 and result[0]['source_tag']==CONTRACT
