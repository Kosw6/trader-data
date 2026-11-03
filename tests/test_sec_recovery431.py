import copy,importlib.util,sys
from pathlib import Path
import pytest
from transformers.sec_transformer import transform_financial_facts,TARGET_FORMS

path=Path(__file__).resolve().parents[2]/'trader-ai/scripts/sec_recovery431.py'
spec=importlib.util.spec_from_file_location('sec_recovery431',path)
worker=importlib.util.module_from_spec(spec);spec.loader.exec_module(worker)

def payload(taxonomy='us-gaap',tag='Assets',unit='USD',**changes):
 fact={'accn':'0000749098-26-000001','end':'2025-12-31','val':123,'fy':2025,'fp':'FY','form':'20-F','filed':'2026-03-01'}
 fact.update(changes)
 return {'cik':749098,'facts':{taxonomy:{tag:{'units':{unit:[fact]}}}}}

def convert(value):return worker.convert_facts(value,'file:///original','2026-10-21T00:00:00+00:00')

@pytest.mark.parametrize('value',[None,True,False,'NaN','Infinity','1e28','-1e28','0.00000000001'])
def test_invalid_numeric_is_never_rounded(value):
 with pytest.raises((ValueError,worker.InvalidOperation)):worker.numeric_value(value)

@pytest.mark.parametrize('value',['9999999999999999999999999999.9999999999','-123.4567890123',0,'1e-10'])
def test_exact_decimal(value):assert worker.Decimal(worker.numeric_value(value))==worker.Decimal(str(value))

def test_ifrs_remains_unapproved_and_original_currency():
 rows,drops=convert(payload('ifrs-full','Assets','CAD'))
 assert not drops and rows[0]['metric_code'] is None
 assert (rows[0]['taxonomy'],rows[0]['source_tag'],rows[0]['unit'])==('ifrs-full','Assets','CAD')

def test_six_k_exact_duration_not_assumed_quarter():
 rows,_=convert(payload(form='6-K',start='2025-01-01',end='2025-08-31',fp=None,fy=None))
 assert (rows[0]['form_type'],rows[0]['period_start'],rows[0]['period_end'],rows[0]['fiscal_period'])==('6-K','2025-01-01','2025-08-31',None)

def test_unknown_us_tag_stored_without_guess():
 rows,_=convert(payload(tag='LiabilitiesAndStockholdersEquity'))
 assert rows[0]['metric_code'] is None

def test_original_hash_matches_default_transformer():
 value=payload(form='10-K');rows,_=convert(value)
 old=transform_financial_facts(value,'file:///original','2026-10-21T00:00:00+00:00')[0]
 assert old['source_fact_hash']==rows[0]['source_fact_hash']
 assert rows[0]['metric_code']=='ASSETS'

def test_payload_not_mutated_and_default_scope_unchanged():
 value=payload(custom_dimension={'x':'y'});before=copy.deepcopy(value)
 rows,_=convert(value)
 assert value==before and TARGET_FORMS=={'10-K','10-Q','8-K'}
 assert rows[0]['dimensions']=={'custom_dimension':{'x':'y'}}

def test_bad_period_and_accession_are_review_not_fabrication():
 for changes,reason in [({'start':'2026-01-01'},'INVALID_PERIOD'),({'accn':'bad'},'INVALID_ACCESSION'),({'val':'1e28'},'NUMERIC_RANGE')]:
  rows,drops=convert(payload(**changes));assert rows==[] and drops=={reason:1}

def test_generation_idempotence_and_preserves_original_identity():
 from collectors.common import build_dedupe_key
 a=worker.source_identity('COMPANY_FACTS','123','hash','sec431/raw/x')
 assert a==worker.source_identity('COMPANY_FACTS','123','hash','sec431/raw/x')
 assert a!=build_dedupe_key('SEC','COMPANY_FACTS','123','hash')
 assert a!=worker.source_identity('COMPANY_FACTS','123','different','sec431/raw/x')

def test_raw_copy_is_immutable(tmp_path,monkeypatch):
 monkeypatch.setattr(worker,'OUT',tmp_path)
 value=payload();first=worker.stage_source(value,'COMPANY_FACTS','123','2026-10-21')
 second=worker.stage_source(value,'COMPANY_FACTS','123','2026-10-21')
 assert first==second
 raw=next(tmp_path.rglob('*.json'));raw.write_text('tampered')
 with pytest.raises(AssertionError,match='RECOVERY_RAW_CONTENT_CHANGED'):
  worker.stage_source(value,'COMPANY_FACTS','123','2026-10-21')

def test_commit_uncertainty_is_safety_pause():assert issubclass(worker.CommitUncertain,worker.SafetyPause)

def test_provider_rate_limit_is_pause_without_retry(monkeypatch):
 from backfills.sec_financial import backfill
 monkeypatch.setattr(backfill,'require_user_agent',lambda:'recovery-test contact@example.invalid')
 client=worker.OfficialClient()
 assert all(a.max_retries.total==0 and not a.max_retries.raise_on_status for a in client.session.adapters.values())
 class Response:status_code=429
 calls=[]
 monkeypatch.setattr(client.session,'get',lambda *a,**k:(calls.append(1) or Response()))
 with pytest.raises(worker.SafetyPause,match='PROVIDER_HTTP_429'):client.get_json('https://data.sec.gov/test')
 assert calls==[1]
