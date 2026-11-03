from datetime import date
from unittest.mock import MagicMock
import json
import pytest
from pipeline import financial_reconciliation677 as module
from pipeline.methode_precision726 import BASIS
from pipeline.reviewed_financial_mapping672 import digest


def review():return dict(companyId=229,metricId=24,metric='OPERATING_INCOME',chosenTag='OperatingIncomeLoss',basis=BASIS,start='2011-05-01',end='2012-04-28')


def test_original_revenue_query_and_parameters_are_preserved():
    r=dict(metric='REVENUE',start='2016-01-01',end='2016-12-31')
    assert module.entry_scope_query(1,r)==(module.SCOPE_SQL,(1,r['start'],r['end']))


def test_named_contract_selects_operating_rows():
    sql,params=module.entry_scope_query(229,review())
    assert "m.metric_code='OPERATING_INCOME'" in sql and "f.unit='USD'" in sql
    assert params==(229,'2011-05-01','2012-04-28')


@pytest.mark.parametrize('key,value',[('companyId',1),('metric','NET_INCOME'),('metric','OPERATING_INCOME; DROP TABLE public.company'),('chosenTag','NetIncomeLoss'),('start','2011-05-02'),('end','2013-04-27')])
def test_registered_scope_cannot_expand_company_metric_tag_or_dates(key,value):
    r=review();r[key]=value
    with pytest.raises(ValueError):module.entry_scope_query(229,r)


def test_other_basis_is_not_a_generic_nonrevenue_route():
    with pytest.raises(ValueError,match='NOT_REGISTERED'):module.entry_scope_query(229,{**review(),'basis':'UNKNOWN'})


def test_nonrevenue_cannot_shadow_revenue_on_same_date(monkeypatch):
    rows=[dict(company_id=229,metric_code='REVENUE',period_start='2011-05-01',period_end='2012-04-28',value='1')]
    cur=MagicMock();cur.description=[(k,) for k in rows[0]];cur.fetchall.return_value=[tuple(r.values()) for r in rows]
    entries=[dict(review=dict(metric='REVENUE',end='2012-04-28',start='2011-05-01',sourceScopeHash=digest(rows))),dict(review={**review(),'sourceScopeHash':'f'*64})]
    import pipeline.reconciliation_review679 as discovery
    monkeypatch.setattr(discovery,'candidate',lambda *a:(_ for _ in ()).throw(AssertionError('registered revenue shadowed')))
    assert module.discover_company(cur,229,date(2012,1,1),date(2012,12,31),entries)==[]


def fixture(tmp_path,monkeypatch,existing=None,changed=False):
    rows=[dict(company_id=229,metric_id=24,metric_code='OPERATING_INCOME',period_start='2011-05-01',period_end='2012-04-28',value='11357000')]
    r={**review(),'sourceScopeHash':digest(rows)};entry=dict(cik='0000065270',review=r,documents=[])
    path=tmp_path/'manifest.json';path.write_text(json.dumps(dict(contract=module.CONTRACT,entries=[entry])),encoding='utf8');monkeypatch.setenv('FINANCIAL_RECONCILIATION_MANIFEST',str(path))
    monkeypatch.setattr(module,'discover_company',lambda *a:[])
    monkeypatch.setattr(module,'validate_entry',lambda *a:dict(proof=dict(policy='test-726'),proof_hash='a'*64))
    if changed:rows[0]['value']='11358000'
    cur=MagicMock();cur.description=[(k,) for k in rows[0]];cur.fetchall.side_effect=[[tuple(v.values()) for v in rows],existing or []]
    cur.fetchone.side_effect=[(77,)] if changed or existing else [None,(77,)]
    return cur


def test_transaction_inserts_only_named_approval_and_receipt(tmp_path,monkeypatch):
    cur=fixture(tmp_path,monkeypatch);r=module.apply_reviewed_company(cur,229,date(2012,1,1),date(2012,12,31),annual_only=True)
    assert r['results']=={'INSERTED':1} and r['receiptId']==77
    sql=[c.args[0] for c in cur.execute.call_args_list]
    assert any("m.metric_code='OPERATING_INCOME'" in q for q in sql)
    assert not any(q.lstrip().startswith(('UPDATE ','DELETE ','INSERT INTO public.')) for q in sql)


def test_named_metric_replay_does_not_insert_again(tmp_path,monkeypatch):
    cur=fixture(tmp_path,monkeypatch,[(True,'OperatingIncomeLoss','a'*64,'test-726')]);r=module.apply_reviewed_company(cur,229,date(2012,1,1),date(2012,12,31))
    assert r['results']=={'EXISTING':1} and not any('INSERT INTO finance_mapping667.approved' in c.args[0] for c in cur.execute.call_args_list)


def test_changed_source_is_held_without_approval(tmp_path,monkeypatch):
    cur=fixture(tmp_path,monkeypatch,changed=True);r=module.apply_reviewed_company(cur,229,date(2012,1,1),date(2012,12,31))
    assert r['results']=={'HELD':1} and r['excluded']=={'RECONCILIATION_SOURCE_CHANGED':1}
    assert not any('INSERT INTO finance_mapping667.approved' in c.args[0] for c in cur.execute.call_args_list)
