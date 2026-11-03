from copy import deepcopy
from decimal import Decimal
import pytest
from collectors.common import content_hash
from pipeline.financial_history674 import legacy_revenue_approval, raw_index, validate_proposal, LEGACY_POLICY
from pipeline.financial_mapping667 import digest
import importlib.util
from pathlib import Path
_spec=importlib.util.spec_from_file_location('legacy693_fixture',Path(__file__).with_name('test_financial_history674.py'))
_fixture=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_fixture)
example,ASOF,SOURCE,CONTRACT=_fixture.example,_fixture.ASOF,_fixture.SOURCE,_fixture.CONTRACT


def legacy():
    rows, payload = example()
    observation = payload['facts']['us-gaap'].pop(CONTRACT)['units']['USD'][0]
    payload['facts']['us-gaap']['SalesRevenueNet']['units']['USD'].append(observation)
    rows[-1]['source_tag'] = 'SalesRevenueNet'
    rows[-1]['source_fact_hash'] = content_hash({'cik':'0001045810','taxonomy':'us-gaap','source_tag':'SalesRevenueNet','unit':'USD','fact':observation})
    return rows, payload


def test_legacy_retains_actual_tag_and_exact_scope_all_raw_sources():
    rows, payload = legacy()
    approval = legacy_revenue_approval(rows, raw_index(payload,1045810), SOURCE, today=ASOF)
    validate_proposal(approval)
    assert approval['policy_version'] == LEGACY_POLICY
    assert approval['source_tag'] == 'SalesRevenueNet'
    assert approval['proof']['mappingScope'] == 'COMPANY_METRIC_PERIOD_END_ONLY'
    assert len(approval['proof']['rawMatchedFactHashes']) == 2
    assert rows == legacy()[0]


def test_supported_common_modern_tag_keeps_previous_policy():
    rows,payload = example()
    with pytest.raises(ValueError, match='LEGACY_SERVING_NOT_NEEDED'):
        legacy_revenue_approval(rows, raw_index(payload,1045810),SOURCE,today=ASOF)


@pytest.mark.parametrize('change,reason',[
    ({'value':Decimal('101')},'RAW_OBSERVATION_MISMATCH'),
    ({'source_fact_hash':'e'*64},'RAW_FACT_NOT_FOUND'),
    ({'report_date':'2025-12-31'},'ORIGINAL_REPORT_NOT_FOUND'),
    ({'dimensions':{'segment':'X'}},'UNREVIEWED_UNIT_TAXONOMY_OR_DIMENSIONS'),
    ({'unit':'EUR'},'UNREVIEWED_UNIT_TAXONOMY_OR_DIMENSIONS'),
    ({'company_id':2},'MIXED_SOURCE_IDENTITY'),
])
def test_legacy_does_not_relax_raw_identity_or_original(change,reason):
    rows,payload=legacy();rows[0].update(change)
    with pytest.raises(ValueError,match=reason):legacy_revenue_approval(rows,raw_index(payload,1045810),SOURCE,today=ASOF)


def test_raw_verified_changed_value_still_needs_restatement_review():
    rows,payload=legacy();obs=payload['facts']['us-gaap']['SalesRevenueNet']['units']['USD'][-1];obs['val']=101
    rows[-1].update(value=Decimal('101'),source_fact_hash=content_hash({'cik':'0001045810','taxonomy':'us-gaap','source_tag':'SalesRevenueNet','unit':'USD','fact':obs}))
    with pytest.raises(ValueError,match='RESTATEMENT_REVIEW_REQUIRED'):legacy_revenue_approval(rows,raw_index(payload,1045810),SOURCE,today=ASOF)


@pytest.mark.parametrize('key',['servingTagContract','mappingScope','originals','sources','sourceScopeHash'])
def test_forged_or_unbound_legacy_proof_rejected(key):
    rows,payload=legacy();d=legacy_revenue_approval(rows,raw_index(payload,1045810),SOURCE,today=ASOF)
    d['proof'].pop(key);d['proof_hash']=digest(d['proof'])
    with pytest.raises(ValueError,match='INVALID_LEGACY_REVENUE_EVIDENCE'):validate_proposal(d)


@pytest.mark.parametrize('enabled',[True,False])
def test_future_collection_uses_opt_in_legacy_hook_without_changing_raw(monkeypatch,enabled):
    from unittest.mock import Mock
    from pipeline import financial_history674 as h
    rows,payload=legacy();original=deepcopy(payload)
    monkeypatch.setenv('FINANCIAL_HISTORY_RAW_ENABLED','true')
    monkeypatch.setenv('FINANCIAL_LEGACY_REVENUE693_ENABLED',str(enabled).lower())
    monkeypatch.setattr(h,'read_rows',lambda *a:rows)
    cur=Mock();cur.fetchall.side_effect=[[],[]];recorder=Mock();monkeypatch.setattr(h,'store_evidence',recorder)
    h.capture_collected_evidence(cur,{'company_id':1,'cik':'0001045810'},
        {'raw_payloads674':[({'content_hash':content_hash(payload),'object_uri':SOURCE['objectUri']},payload)]})
    proposals=recorder.call_args.args[2]
    assert len(proposals)==int(enabled)
    if proposals:assert proposals[0]['policy_version']==LEGACY_POLICY
    assert payload==original
