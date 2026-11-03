import hashlib
from copy import deepcopy
import pytest
import pipeline.yelp_source720 as module
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest


def fixture(monkeypatch,old=None,new=None):
    rows=[[''],[''],['As Previously Reported','Impact of ASC 606 Adoption','As Currently Reported'],['Consolidated Statement of Operations—Year Ended December 31, 2016'],['Net revenue','713069','2994','716063']]+[['Other statement row'] for _ in range(7)]+[['Consolidated Statement of Operations—Year Ended December 31, 2017'],['Net revenue','846813','4034','850847']]
    context='The Company adopted ASC 606 effective January 1, 2018 using the full retrospective method and, accordingly, has recast each prior reporting period presented. The following adjustments to its previously reported results (in thousands):'
    body=('<p>'+context+'</p><table>'+''.join('<tr>'+''.join('<td>'+c+'</td>' for c in row)+'</tr>' for row in rows)+'</table>').encode()
    if old:body=body.replace(old.encode(),new.encode())
    p=Tables();p.feed(body.decode());monkeypatch.setattr(module,'SOURCE_SHA256',hashlib.sha256(body).hexdigest());monkeypatch.setattr(module,'TABLE_SHA256',digest(p.tables[0]));return body


def test_both_actual_annual_columns_method_units_and_equations_are_source_bound(monkeypatch):
    rows=module.source_evidence(fixture(monkeypatch));assert len(rows)==2
    assert [r['current'] for r in rows]==['716063000','850847000']
    assert all(r['method']=='ASC606_FULL_RETROSPECTIVE_EXPLICIT' and not r['approvalGenerated'] and not r['publicationAllowed'] and 'CURRENT_DB_SCOPE_REVALIDATION' in r['requires'] for r in rows)


@pytest.mark.parametrize('old,new',[('full retrospective','modified retrospective'),('recast each prior reporting period presented','no prior period changes'),('in thousands','in millions'),('As Currently Reported','As Reported'),('Impact of ASC 606 Adoption','Adjustment of unknown cause'),('December 31, 2017','December 30, 2017'),('Year Ended December 31, 2016','Three Months Ended December 31, 2016'),('4034','4035'),('Net revenue','Other income')])
def test_rehashed_semantic_changes_do_not_clear_source_review(monkeypatch,old,new):
    with pytest.raises(ValueError):module.source_evidence(fixture(monkeypatch,old,new))


def scope():
    rows=[dict(company_id=2149,cik='0001345016',metric_id=22,metric_code='REVENUE',period_start='2016-01-01',period_end='2016-12-31',taxonomy='us-gaap',unit='USD',dimensions=None,form_type='10-K',source_url=module.URL,source_tag=tag,value=value,source_fact_hash=letter*64) for tag,value,letter in [('SalesRevenueNet','713069000','a'),('RevenueFromContractWithCustomerExcludingAssessedTax','716063000','b')]]
    return dict(companyId=2149,metricId=22,cik='0001345016',metric='REVENUE',end='2016-12-31',rows=rows,sourceScopeHash=digest(rows))


def test_frozen_raw_bind_is_immutable_and_never_claims_a_current_database_read(monkeypatch):
    evidence=module.source_evidence(fixture(monkeypatch))[0];s=scope();before=deepcopy(s);r=module.bind_frozen_scope(s,evidence)
    assert s==before and r['frozenRawRows']==2 and not r['currentDatabaseRevalidated'] and not r['publicationAllowed']


@pytest.mark.parametrize('change',['company','hash','value','start','unit','dimension'])
def test_wrong_frozen_identity_or_raw_semantics_are_rejected(monkeypatch,change):
    evidence=module.source_evidence(fixture(monkeypatch))[0];s=scope()
    if change=='company':s['companyId']=1
    elif change=='hash':s['sourceScopeHash']='f'*64
    else:
        k,v={'value':('value','713000000'),'start':('period_start','2016-10-01'),'unit':('unit','EUR'),'dimension':('dimensions',{'segment':'a'})}[change];s['rows'][0][k]=v;s['sourceScopeHash']=digest(s['rows'])
    with pytest.raises(ValueError):module.bind_frozen_scope(s,evidence)
