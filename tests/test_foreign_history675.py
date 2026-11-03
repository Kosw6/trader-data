from copy import deepcopy
from datetime import date
from unittest.mock import Mock
import pytest
import importlib.util
from pathlib import Path
_spec=importlib.util.spec_from_file_location('history674_test_fixture',Path(__file__).with_name('test_financial_history674.py'))
_fixture=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_fixture)
example,SOURCE,ASOF=_fixture.example,_fixture.SOURCE,_fixture.ASOF
from collectors.common import content_hash
from pipeline import financial_history674 as h

def foreign(code='REVENUE',form='20-F'):
 rows,payload=example(code);source=deepcopy(SOURCE);source['foreignAnnualReview']={}
 for row in rows:
  obs=next(o for o in payload['facts']['us-gaap'][row['source_tag']]['units'][row['unit']] if o['accn']==row['accession_number'])
  obs['form']=form;row['form_type']=form
  row['source_fact_hash']=content_hash({'cik':row['cik'],'taxonomy':row['taxonomy'],'source_tag':row['source_tag'],'unit':row['unit'],'fact':obs})
  source['foreignAnnualReview'][row['accession_number']]={'basis':'US_GAAP_PRIMARY_FINANCIAL_STATEMENTS','cik':row['cik'],'url':row['source_url'],'documentSha256':'a'*64,'auditorOpinionExcerpt':'Synthetic test fixture auditor opinion in conformity with U.S. GAAP.'}
 return rows,payload,source

@pytest.mark.parametrize('form',['20-F','20-F/A','40-F','40-F/A'])
@pytest.mark.parametrize('code',['REVENUE','ASSETS'])
def test_foreign_annual_only_with_raw_values_and_explicit_document_review(form,code):
 rows,payload,source=foreign(code,form);before=deepcopy(rows)
 d=h.unchanged_approval(rows,h.raw_index(payload,1045810),source,today=ASOF,foreign_annual=True)
 assert d['proof']['annualFormContract']=='SEC_FOREIGN_ANNUAL_US_GAAP675_V1' and d['proof']['validatedForms']==[form]
 assert rows==before and d['source_tag']!='SalesRevenueNet'

@pytest.mark.parametrize('change',[{'basis':'IFRS'}, {'cik':'0000000001'},{'url':'https://evil.invalid/a'}, {'documentSha256':'bad'},{'auditorOpinionExcerpt':''}])
def test_foreign_annual_cannot_assume_primary_us_gaap_from_taxonomy_alone(change):
 rows,payload,source=foreign();source['foreignAnnualReview'][rows[0]['accession_number']].update(change)
 with pytest.raises(ValueError,match='FOREIGN_ANNUAL_BASIS_DOCUMENT_REVIEW_REQUIRED'):
  h.unchanged_approval(rows,h.raw_index(payload,1045810),source,today=ASOF,foreign_annual=True)

def test_foreign_review_does_not_relax_original_report_or_restatement_guard():
 rows,payload,source=foreign();rows[0]['report_date']='2025-12-31'
 with pytest.raises(ValueError,match='ORIGINAL_REPORT_NOT_FOUND'):h.unchanged_approval(rows,h.raw_index(payload,1045810),source,today=ASOF,foreign_annual=True)
 rows,payload,source=foreign();rows[-1]['value']='101'
 with pytest.raises(ValueError,match='RAW_OBSERVATION_MISMATCH'):h.unchanged_approval(rows,h.raw_index(payload,1045810),source,today=ASOF,foreign_annual=True)

def test_6k_does_not_become_an_annual_or_quarter_report():
 rows,payload,source=foreign(form='6-K')
 with pytest.raises(ValueError,match='NO_SUPPORTED_PERIOD'):h.unchanged_approval(rows,h.raw_index(payload,1045810),source,today=ASOF,foreign_annual=True)

def test_reviewed_foreign_annual_reaches_go_annual_scope(monkeypatch):
 rows,payload,source=foreign();d=h.unchanged_approval(rows,h.raw_index(payload,1045810),source,today=ASOF,foreign_annual=True)
 monkeypatch.setenv('FINANCIAL_HISTORY_RAW_ENABLED','true');monkeypatch.setattr(h,'read_rows',lambda *a:rows)
 cur=Mock();cur.fetchone.return_value=({'proposals674':[d]},);cur.fetchall.side_effect=[[],[]]
 assert len(h.merge_verified_history(cur,1,date(2024,1,1),ASOF,True,[],repair_only=True))==1

def test_foreign_quarter_duration_cannot_use_reviewed_annual_form():
 rows,payload,source=foreign()
 for row in rows:row['period_start']='2024-10-01'
 with pytest.raises(ValueError,match='NO_SUPPORTED_PERIOD'):h.unchanged_approval(rows,h.raw_index(payload,1045810),source,today=ASOF,foreign_annual=True)

def test_future_raw_collection_reuses_only_reviewed_document_accessions(monkeypatch):
 rows,payload,source=foreign();d=h.unchanged_approval(rows,h.raw_index(payload,1045810),source,today=ASOF,foreign_annual=True)
 monkeypatch.setenv('FINANCIAL_HISTORY_RAW_ENABLED','true');monkeypatch.setattr(h,'read_rows',lambda *a:rows)
 recorder=Mock();monkeypatch.setattr(h,'store_evidence',recorder)
 cur=Mock();cur.fetchall.side_effect=[[],[],[(d['proof'],)]]
 h.capture_collected_evidence(cur,{'company_id':1,'cik':'0001045810'},{'raw_payloads674':[({'content_hash':content_hash(payload),'object_uri':source['objectUri']},payload)]})
 assert len(recorder.call_args.args[2])==1
 assert recorder.call_args.args[2][0]['proof']['rawSource']['foreignAnnualReview']==source['foreignAnnualReview']
