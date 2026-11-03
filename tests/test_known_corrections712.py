import hashlib
from copy import deepcopy
import pytest
import pipeline.known_corrections712 as policy
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest

def fixture(monkeypatch,change=None):
    rows=[[''],['','Year Ended December 31,'],['','2017','2016'],['',*(['As Previously Reported','Corrections','As Corrected']*2)],[''],['','(in millions, except per share amounts)'],['Consolidated Statements of Income Data:'],['Total revenue','1494.9','2.8','1497.7','1275.4','3.1','1278.5'],['Total gross profit','1109.6','2.8','1112.4','937.6','3.1','940.7'],['Operating income','109.8','2.8','112.6','42.9','3.1','46.0'],['Net income','31.4','4.0','35.4','32.2','1.9','34.1'],['Net income per share:'],['Basic','0.18','0.02','0.20','0.19','0.01','0.20'],['Diluted','0.18','0.02','0.20','0.18','0.01','0.19']]
    cause='Immaterial error in revenue recognition for FortiCare support service contracts caused understatement of revenue.'
    if change:change(rows)
    tr=lambda r:'<tr>'+''.join('<td>'+str(c)+'</td>' for c in r)+'</tr>'
    body=('<p>'+cause+'</p><table>'+''.join(tr(r) for r in rows)+'</table>').encode();p=Tables();p.feed(body.decode());monkeypatch.setattr(policy,'SOURCE_SHA256',hashlib.sha256(body).hexdigest());monkeypatch.setattr(policy,'TABLE_SHA256',digest(p.tables[0]));return body

def test_eight_exact_correction_scopes_and_eps_scale_are_derived_from_all_columns(monkeypatch):
    body=fixture(monkeypatch);items=policy.verify_source(body);assert len(items)==8 and all(not x['approvalGenerated'] for x in items)
    eps=next(x for x in items if x['metric']=='DILUTED_EPS' and x['end']=='2017-12-31');assert eps['corrected']=='0.20' and eps['unit']=='USD/shares'
    income=next(x for x in items if x['metric']=='NET_INCOME' and x['end']=='2016-12-31');assert income['corrected']=='34100000.0'

@pytest.mark.parametrize('change',[
    lambda r:r[2].__setitem__(2,'2017'),lambda r:r[3].__setitem__(2,'As Corrected'),lambda r:r[5].__setitem__(1,'(in thousands)'),
    lambda r:r[10].__setitem__(6,'34.2'),lambda r:r[11].__setitem__(0,'Net revenue per share:'),lambda r:r[13].__setitem__(0,'Basic'),
    lambda r:r[7].__setitem__(0,'Service revenue'),lambda r:r[9].pop(),
])
def test_rehashed_wrong_year_headers_units_metric_and_other_year_equations_fail_closed(monkeypatch,change):
    body=fixture(monkeypatch,change)
    with pytest.raises(ValueError):policy.verify_source(body)

def test_source_bytes_are_immutable_even_if_visible_numbers_look_unchanged(monkeypatch):
    body=fixture(monkeypatch)
    with pytest.raises(ValueError,match='SOURCE_CHANGED'):policy.verify_source(body+b' ')
