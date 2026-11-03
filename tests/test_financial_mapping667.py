from datetime import date
from decimal import Decimal
import pytest
from pipeline.financial_mapping667 import invalid,proposals,RULES,enqueue_if_enabled
TODAY=date(2026,10,30)
def fact(**kw):
 r=dict(fact_id=1,company_id=1,metric_id=2,metric_code='REVENUE',taxonomy='us-gaap',source_tag='Revenues',unit='USD',period_start='2024-01-01',period_end='2024-12-31',value=Decimal('100.000'),dimensions=None,source_fact_hash='a'*64,accession_number='0001045810-25-000001',filing_date='2025-02-01',report_date='2024-12-31',source_url='https://www.sec.gov/Archives/edgar/data/1045810/000104581025000001/annual.htm',form_type='10-K',cik='0001045810');r.update(kw);return r
def decide(rows,maps=()):return proposals(rows,list(maps),today=TODAY)[0]
@pytest.mark.parametrize('code',list(RULES))
def test_six_exact_policy_metrics(code):
 unit,tags=RULES[code];r=fact(metric_code=code,unit=unit,source_tag=sorted(tags)[0])
 if code in {'ASSETS','LIABILITIES'}:r['period_start']=None
 d=decide([r])[0];assert d['outcome']=='APPROVE' and d['end']=='2024-12-31'
@pytest.mark.parametrize('changes',[{'unit':'EUR'},{'taxonomy':'ifrs-full'},{'dimensions':{'segment':'cloud'}},{'source_tag':'RevenueFromContractWithCustomerIncludingAssessedTax'},{'report_date':'2025-12-31'},{'value':Decimal('NaN')},{'source_fact_hash':None},{'source_url':'https://www.sec.gov.evil.invalid/a.htm'},{'source_url':'https://www.sec.gov/Archives/edgar/data/999/000104581025000001/a.htm'},{'filing_date':'2028-01-01'},{'form_type':'8-K'},{'metric_code':'CASH'}])
def test_unreviewed_evidence_never_creates_mapping(changes):
 assert invalid(fact(**changes),today=TODAY) is not None;assert decide([fact(**changes)])==[]
def test_aliases_are_not_automatically_merged():
 a=fact();b=fact(fact_id=2,source_tag='RevenueFromContractWithCustomerExcludingAssessedTax')
 assert decide([a,b])[0]['reason']=='SOURCE_TAG_CONFLICT'
 reviewed=[dict(metric_id=2,company_id=1,taxonomy='us-gaap',source_tag='Revenues',valid_from='2024-12-31',valid_to='2024-12-31')]
 assert decide([a,b],reviewed)[0]['outcome']=='EXISTING'
 b['value']=Decimal('101');assert decide([a,b],reviewed)[0]['reason']=='ALIAS_VALUE_CONFLICT'
def test_latest_filing_conflict_and_valid_amendment():
 a=fact();b=fact(fact_id=2,value=Decimal('101'))
 assert decide([a,b])[0]['reason']=='LATEST_FILING_VALUE_CONFLICT'
 b['filing_date']='2025-03-01';d=decide([a,b])[0]
 assert d['outcome']=='APPROVE' and d['proof']['sources'][0]['value']=='101'
def test_specific_mapping_priority_and_conflict():
 mapping=dict(metric_id=2,company_id=None,taxonomy='us-gaap',source_tag='Wrong',valid_from=None,valid_to=None);specific=dict(mapping,company_id=1,source_tag='Revenues')
 assert decide([fact()],[mapping,specific])[0]['outcome']=='EXISTING'
 assert decide([fact()],[specific,dict(specific,source_tag='Other')])[0]['reason']=='EXISTING_MAPPING_CONFLICT'
 assert decide([fact()],[dict(specific,valid_to='2023-12-31')])[0]['outcome']=='APPROVE'
def test_hash_is_stable_for_db_order():
 a=fact();b=fact(fact_id=2,source_fact_hash='b'*64)
 assert decide([a,b])[0]['proof_hash']==decide([b,a])[0]['proof_hash']
def test_annual_mode_does_not_approve_quarters():
 r=fact(period_start='2024-10-01',form_type='10-Q')
 assert invalid(r,today=TODAY) is None
 assert invalid(r,today=TODAY,annual_only=True)=='NON_ANNUAL_SCOPE'
def test_hook_is_opt_in_and_transaction_bound(monkeypatch):
 from unittest.mock import Mock
 cur=Mock();monkeypatch.delenv('FINANCIAL_MAPPING_QUEUE_ENABLED',raising=False);enqueue_if_enabled(cur,1);cur.execute.assert_not_called()
 monkeypatch.setenv('FINANCIAL_MAPPING_QUEUE_ENABLED','true');enqueue_if_enabled(cur,1)
 assert 'finance_mapping667.queue' in cur.execute.call_args.args[0]


@pytest.mark.parametrize("fail_before_hook", [False, True])
def test_common_sec_etl_enqueues_only_inside_successful_data_transaction(monkeypatch, fail_before_hook):
 from collections import Counter
 from datetime import datetime,timezone
 from unittest.mock import MagicMock,Mock
 from backfills.sec_financial import backfill
 connection=MagicMock();cursor=connection.cursor.return_value.__enter__.return_value
 monkeypatch.setattr(backfill,'seed_financial_metrics',Mock(return_value={}))
 monkeypatch.setattr(backfill,'upsert_filings',Mock(return_value={}))
 fact_writer=Mock(side_effect=RuntimeError('FACT_WRITE_FAILED')) if fail_before_hook else Mock(return_value={})
 monkeypatch.setattr(backfill,'upsert_financial_facts',fact_writer)
 hook=Mock();monkeypatch.setattr('pipeline.financial_mapping667.enqueue_if_enabled',hook)
 records={'source_object':[],'filing':[],'financial_fact':[]}
 if fail_before_hook:
  with pytest.raises(RuntimeError,match='FACT_WRITE_FAILED'):
   backfill.apply_company_financials(connection,{'company_id':1440},records,datetime.now(timezone.utc),Counter(),[])
  hook.assert_not_called();connection.commit.assert_not_called()
 else:
  def assert_before_commit(actual_cursor,company):
   assert actual_cursor is cursor and company==1440
   connection.commit.assert_not_called()
  hook.side_effect=assert_before_commit
  backfill.apply_company_financials(connection,{'company_id':1440},records,datetime.now(timezone.utc),Counter(),[])
  hook.assert_called_once_with(cursor,1440);connection.commit.assert_called_once()


def test_future_collection_reads_isolated_schema_with_canonical_metric_ids():
 from unittest.mock import Mock
 from pipeline.financial_mapping667 import read_company
 row=fact(fact_id='control649:10',metric_id=2)
 cur=Mock();cur.description=[(k,) for k in row]
 # Public has no new period yet; collection uses independent physical metric IDs.
 cur.fetchall.side_effect=[[],[tuple(row.values())],[]]
 cur.fetchone.return_value=('0001045810','0001045810')
 result,excluded=read_company(cur,1,date(2014,1,1),TODAY)
 assert result[0]['outcome']=='APPROVE' and result[0]['metric_id']==2
 assert result[0]['proof']['sources'][0]['factId']=='control649:10'
 sql=[call.args[0] for call in cur.execute.call_args_list]
 assert any('pm.metric_code=sm.metric_code' in x and 'canonical.cik=c.cik' in x for x in sql)


def test_future_collection_identity_mismatch_is_not_mapped():
 from unittest.mock import Mock
 from pipeline.financial_mapping667 import read_company
 cur=Mock();cur.description=[];cur.fetchall.return_value=[];cur.fetchone.return_value=('0001045810','0000000001')
 with pytest.raises(ValueError,match='MAPPING_COMPANY_IDENTITY_UNVERIFIED'):
  read_company(cur,1,date(2014,1,1),TODAY)
