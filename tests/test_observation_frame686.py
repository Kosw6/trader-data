from copy import deepcopy
from datetime import date
from decimal import Decimal
import pytest
from collectors.common import content_hash
from pipeline.financial_history674 import raw_index,unchanged_approval
from pipeline.observation_frame686 import frame_verified_approval,validate_proposal,POLICY,fact_hash
from pipeline.financial_mapping667 import digest
from tests.test_financial_history674 import example
from unittest.mock import Mock


def case(code='NET_INCOME', old_frame='CY2024', new_frame=None):
    rows,payload=example(code);retained={}
    for row in rows:
        obs=next(o for o in payload['facts']['us-gaap'][row['source_tag']]['units'][row['unit']] if o['accn']==row['accession_number'])
        if old_frame is not None:obs['frame']=old_frame
        row['source_fact_hash']=fact_hash(row['cik'],row['source_tag'],row['unit'],obs)
        retained[row['source_fact_hash']]=old_frame
        obs.pop('frame',None)
        if new_frame is not None:obs['frame']=new_frame
    source={'canonicalJsonSha256':content_hash(payload),'objectUri':'s3://verified-original-archive/companyfacts.json'}
    return rows,payload,source,retained


@pytest.mark.parametrize('code',['REVENUE','NET_INCOME','OPERATING_INCOME','ASSETS','LIABILITIES','DILUTED_EPS'])
@pytest.mark.parametrize('old,new',[('CY2024',None),(None,'CY2024'),('CY2024Q4','CY2024')])
def test_only_optional_frame_evolution_is_proven_without_editing_originals(code,old,new):
    rows,payload,source,frames=case(code,old,new);before=deepcopy((rows,payload))
    with pytest.raises(ValueError,match='RAW_FACT_NOT_FOUND'):unchanged_approval(rows,raw_index(payload,1045810),source,today=date(2026,10,31))
    a=frame_verified_approval(rows,payload,source,frames,today=date(2026,10,31));validate_proposal(a)
    assert a['policy_version']==POLICY and (rows,payload)==before
    assert a['proof']['rawMatchedFactHashes']!=a['proof']['storedMatchedFactHashes']
    assert a['proof_hash']==digest(a['proof'])


@pytest.mark.parametrize('field,value',[('val',101),('fy',2030),('fp','Q1'),('form','10-Q'),('filed','2025-03-01'),('accn','0001045810-25-000009'),('start','2024-01-02'),('end','2024-12-30')])
def test_frame_contract_cannot_hide_any_other_observation_change(field,value):
    rows,payload,source,frames=case();obs=payload['facts']['us-gaap'][rows[0]['source_tag']]['units']['USD'][0];obs[field]=value;source['canonicalJsonSha256']=content_hash(payload)
    with pytest.raises(ValueError):frame_verified_approval(rows,payload,source,frames,today=date(2026,10,31))


def test_restatement_and_missing_original_are_still_held():
    rows,payload,source,frames=case();rows[0]['report_date']='2023-12-31'
    with pytest.raises(ValueError,match='ORIGINAL_REPORT_NOT_FOUND'):frame_verified_approval(rows,payload,source,frames,today=date(2026,10,31))


def test_retained_frame_is_required_and_must_reconstruct_the_exact_hash():
    rows,payload,source,frames=case()
    with pytest.raises(ValueError,match='RETAINED'):frame_verified_approval(rows,payload,source,{},today=date(2026,10,31))
    frames={h:'CY2023' for h in frames}
    with pytest.raises(ValueError,match='UNIQUE'):frame_verified_approval(rows,payload,source,frames,today=date(2026,10,31))


@pytest.mark.parametrize('field,value',[('storedFrame','CY2023'),('storedHash','0'*64),('rawFullHash','0'*64),('unit','EUR'),('tag','OtherTag')])
def test_changed_prepared_binding_is_rejected_even_if_outer_digest_is_recomputed(field,value):
    rows,payload,source,frames=case();a=frame_verified_approval(rows,payload,source,frames,today=date(2026,10,31));a['proof']['rawObservationFrameBindings'][0][field]=value;a['proof_hash']=digest(a['proof'])
    with pytest.raises(ValueError):validate_proposal(a)


def test_source_payload_must_match_preserved_raw_descriptor():
    rows,payload,source,frames=case();source['canonicalJsonSha256']='0'*64
    with pytest.raises(ValueError,match='SOURCE_HASH'):frame_verified_approval(rows,payload,source,frames,today=date(2026,10,31))


@pytest.mark.parametrize('enabled',[True,False])
def test_future_collector_uses_exact_frame_contract_only_when_enabled(monkeypatch,enabled):
    from pipeline import financial_history674 as h
    rows,payload,source,frames=case();before=deepcopy(rows)
    monkeypatch.setenv('FINANCIAL_HISTORY_RAW_ENABLED','true');monkeypatch.setenv('FINANCIAL_FRAME_METADATA_ENABLED',str(enabled).lower())
    monkeypatch.setattr(h,'read_rows',lambda *a:rows);reader=Mock(return_value=frames);monkeypatch.setattr(h,'retained_frame_map',reader)
    recorder=Mock();monkeypatch.setattr(h,'store_evidence',recorder)
    cur=Mock();cur.fetchall.side_effect=[[],[]]
    h.capture_collected_evidence(cur,{'company_id':1,'cik':'0001045810'}, {'raw_payloads674':[({'content_hash':content_hash(payload),'object_uri':source['objectUri']},payload)]})
    proposals=recorder.call_args.args[2]
    assert len(proposals)==int(enabled) and rows==before
    if enabled:
        h.validate_proposal(proposals[0]);assert proposals[0]['policy_version']==POLICY
        reader.assert_called_once()
    else:reader.assert_not_called()


@pytest.mark.parametrize('prior_type',['none','same','old674','inactive','changed_scope'])
def test_mapper_replays_only_current_frame_proof_and_preserves_older_policy(monkeypatch,prior_type):
    from pipeline import financial_history674 as h
    rows,payload,source,frames=case();a=frame_verified_approval(rows,payload,source,frames,today=date(2026,10,31))
    monkeypatch.setenv('FINANCIAL_HISTORY_RAW_ENABLED','true');monkeypatch.setattr(h,'read_rows',lambda *a:rows)
    prior=[];mappings=[]
    if prior_type in {'same','inactive','changed_scope'}:
        prior=[(2,date(2024,12,31),a['source_tag'],prior_type!='inactive',POLICY,a['proof'],a['proof_hash'])]
        mappings=[(2,1,'us-gaap',a['source_tag'],date(2024,12,31),date(2024,12,31))]
    if prior_type=='old674':prior=[(2,date(2024,12,31),a['source_tag'],True,h.POLICY,{'metricCode':'NET_INCOME'},'0'*64)]
    if prior_type=='changed_scope':rows[0]['value']='101'
    cur=Mock();cur.fetchone.return_value=({'proposals674':[a]},);cur.fetchall.side_effect=[prior,mappings]
    if prior_type=='old674':
        # A preexisting different contract is excluded before it is appended;
        # here prevent the deliberately minimal synthetic old proof from
        # reaching its unrelated legacy validation fixture.
        prior[0]=(2,date(2024,12,31),a['source_tag'],True,'operator-reviewed-old',{},'0'*64)
    result=h.merge_verified_history(cur,1,date(2024,1,1),date(2026,10,31),False,[],repair_only=True)
    if prior_type in {'inactive','old674'}:assert result==[]
    else:
        assert len(result)==1
        assert result[0]['outcome']=={'none':'APPROVE','same':'EXISTING','changed_scope':'HELD'}[prior_type]
    assert not any('INSERT' in c.args[0] or 'UPDATE' in c.args[0] for c in cur.execute.call_args_list)


def test_frame_metadata_reads_are_partition_pruned_and_conflicts_are_held():
    from pipeline.financial_history674 import retained_frame_map
    rows,_,_,_=case();h=rows[0]['source_fact_hash'];cur=Mock();cur.fetchall.side_effect=[[(h,'CY2024')],[(h,'CY2023')]]
    with pytest.raises(ValueError,match='CONFLICT'):retained_frame_map(cur,1,rows)
    assert all('company_id=%s' in c.args[0] for c in cur.execute.call_args_list)


def test_existing_674_proof_survives_new_frame_candidate_without_replacement(monkeypatch):
    from pipeline import financial_history674 as h
    rows,payload,source,frames=case();a=frame_verified_approval(rows,payload,source,frames,today=date(2026,10,31))
    old_payload=deepcopy(payload)
    for concepts in old_payload['facts']['us-gaap'].values():
        for observations in concepts['units'].values():
            for obs in observations:obs['frame']='CY2024'
    old_source={**source,'canonicalJsonSha256':content_hash(old_payload)}
    old=unchanged_approval(rows,raw_index(old_payload,1045810),old_source,today=date(2026,10,31))
    monkeypatch.setenv('FINANCIAL_HISTORY_RAW_ENABLED','true');monkeypatch.setattr(h,'read_rows',lambda *a:rows)
    cur=Mock();cur.fetchone.return_value=({'proposals674':[a]},);cur.fetchall.side_effect=[
        [(2,date(2024,12,31),old['source_tag'],True,h.POLICY,old['proof'],old['proof_hash'])],
        [(2,1,'us-gaap',old['source_tag'],date(2024,12,31),date(2024,12,31))]]
    result=h.merge_verified_history(cur,1,date(2024,1,1),date(2026,10,31),False,[],repair_only=True)
    assert len(result)==1 and result[0]['outcome']=='EXISTING' and result[0]['proof_hash']==old['proof_hash'] and result[0]['policy_version']==h.POLICY
