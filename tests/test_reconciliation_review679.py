import gzip,json
from copy import deepcopy
import pytest
from unittest.mock import MagicMock
from pipeline.reconciliation_review679 import candidate,review_cards,numeric
from workers.review_reconciliation679 import run
from pipeline.financial_reconciliation677 import discover_company
from pipeline.reviewed_financial_mapping672 import digest

def scope():
    r={'company_id':1,'cik':'1','taxonomy':'us-gaap','unit':'USD','dimensions':None,'period_start':'2016-01-01','period_end':'2016-12-31','form_type':'10-K','report_date':'2016-12-31','filing_date':'2017-02-01','source_tag':'SalesRevenueNet','value':'100000000','source_url':'https://www.sec.gov/Archives/edgar/data/1/000000000117000001/original.htm'}
    later=dict(r,report_date='2018-12-31',filing_date='2019-02-01',source_tag='Revenues',value='110000000',source_url='https://www.sec.gov/Archives/edgar/data/1/000000000119000001/later.htm')
    return {'companyId':1,'cik':'1','metric':'REVENUE','end':'2016-12-31','rows':[r,later]}

def test_candidates_never_generate_approvals():
    s=scope();before=deepcopy(s);r=candidate(s)
    assert r['adjustment']=='10000000' and r['requiresHumanReview'] and not r['approvalGenerated'] and s==before

def test_invalid_period_remains_outside_review_candidates():
    s=scope();s['rows'][0]['period_start']='2016-02-31';assert candidate(s) is None

@pytest.mark.parametrize('case',['same','conflict','no_original','currency','dimension','metric','identity'])
def test_ambiguous_or_inapplicable_candidates_are_excluded(case):
    s=scope()
    if case=='same':s['rows'][1]['value']=s['rows'][0]['value']
    if case=='conflict':s['rows'].append(dict(s['rows'][1],value='111000000'))
    if case=='no_original':s['rows'][0]['report_date']='2017-12-31'
    if case=='currency':s['rows'][0]['unit']='EUR'
    if case=='dimension':s['rows'][0]['dimensions']={'segment':'one'}
    if case=='metric':s['metric']='NET_INCOME'
    if case=='identity':s['rows'][0]['company_id']=2
    assert candidate(s) is None

@pytest.mark.parametrize('components',['<td>10</td>','<td>7</td><td>3</td>','<td>(5)</td><td>15</td>'])
def test_table_arithmetic_is_only_a_review_aid(components):
    html=('<table><tr><th>2016 (in millions)</th><th>As reported</th><th>As recast</th></tr><tr><td>Total operating revenues</td><td>100</td>'+components+'<td>110</td></tr></table>').encode()
    cards=review_cards(html,candidate(scope()))
    assert len(cards)==1 and cards[0]['numericEquationMatches'] and cards[0]['requiresHumanReview'] and cards[0]['approvalGenerated']==False

def test_expense_row_or_script_text_cannot_be_revenue_evidence():
    body=b'<table><tr><th>In millions As Reported As Adjusted</th></tr><tr><td>Cost of sales</td><td>100</td><td>10</td><td>110</td></tr></table><script>Revenue 100 10 110</script>'
    assert not review_cards(body,candidate(scope()))

def test_explicit_table_units_override_preceding_units():
    body=b'<p>In millions</p><table><tr><th>In thousands As Reported As Adjusted</th></tr><tr><td>Sales</td><td>100000</td><td>10000</td><td>110000</td></tr></table>'
    assert review_cards(body,candidate(scope()))[0]['scale']=='1000'

def test_offline_discovery_only_writes_review_artifacts(tmp_path):
    source=tmp_path/'inventory.gz'
    with gzip.open(source,'wt',encoding='utf8') as f:f.write(json.dumps(scope())+'\n')
    report=run(source,tmp_path/'output',set(), '2016-01-01','2016-12-31',1)
    assert report['candidates']==1 and report['approvalsGenerated']==report['databaseWrites']==0 and report['requiresHumanReview']

def test_queue_discovery_records_only_unreviewed_scopes():
    rows=scope()['rows'];cur=MagicMock();cur.description=[(k,) for k in rows[0]];cur.fetchall.return_value=[tuple(r.values()) for r in rows]
    result=discover_company(cur,1,'2016-01-01','2016-12-31',[])
    assert len(result)==1 and result[0]['requiresHumanReview'] and not result[0]['approvalGenerated']
    registered=[{'review':{'end':'2016-12-31','start':'2016-01-01','sourceScopeHash':digest(rows)}}]
    assert discover_company(cur,1,'2016-01-01','2016-12-31',registered)==[]
    assert all(c.args[0].lstrip().startswith('SELECT ') for c in cur.execute.call_args_list)

def test_new_filing_is_rediscovered_even_when_amount_is_unchanged():
    rows=scope()['rows'];registered=[{'review':{'end':'2016-12-31','start':'2016-01-01','sourceScopeHash':digest(rows)}}]
    rows=rows+[dict(rows[1],filing_date='2020-02-01')];cur=MagicMock();cur.description=[(k,) for k in rows[0]];cur.fetchall.return_value=[tuple(r.values()) for r in rows]
    assert len(discover_company(cur,1,'2016-01-01','2016-12-31',registered))==1


def test_explicit_as_revised_is_discovered_for_review_never_approved():
    body=b'<table><tr><th>2016 (in millions)</th><th>As Previously Reported</th><th>Adjustments</th><th>As Revised</th></tr><tr><td>Net sales</td><td>100</td><td>10</td><td>110</td></tr></table>'
    cards=review_cards(body,candidate(scope()))
    assert len(cards)==1 and cards[0]['numericEquationMatches']
    assert cards[0]['requiresHumanReview'] and cards[0]['approvalGenerated'] is False
