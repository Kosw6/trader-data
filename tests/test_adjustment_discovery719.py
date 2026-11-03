import hashlib
from copy import deepcopy
import pytest
from pipeline.adjustment_discovery719 import discover

URL='https://www.sec.gov/Archives/edgar/data/1/000000000118000001/annual.htm'
ITEM=dict(cik='0000000001',end='2017-12-31',original='846813000',adjustment='4034000',chosen='850847000')


def body(unit='in thousands',new='As Currently Reported',bad=None):
    rows=[['As Previously Reported','Impact of ASC 606 Adoption',new],['Consolidated Statement of Operations—Year Ended December 31, 2016'],['Net revenue','713069','2994','716063'],['Consolidated Statement of Operations—Year Ended December 31, 2017'],['Net revenue','846813','4034','850847']]
    text='<p>ASC 606 review ('+unit+')</p><table>'+''.join('<tr>'+''.join('<td>'+c+'</td>' for c in r)+'</tr>' for r in rows)+'</table>'
    if bad:text=text.replace(*bad)
    return text.encode()


def run(data,item=ITEM):return discover(data,item,URL,hashlib.sha256(data).hexdigest())


@pytest.mark.parametrize('new',['As Currently Reported','As Adjusted'])
def test_current_reported_alias_uses_closest_actual_period_not_first_period(new):
    data=body(new=new);before=deepcopy(ITEM);cards=run(data)
    assert len(cards)==1 and cards[0]['valueRow']==4 and cards[0]['periodHeadingRow']==3 and cards[0]['unitBound']
    assert cards[0]['periodEnd']=='2017-12-31' and not cards[0]['periodStartBound'] and not cards[0]['publicationAllowed'] and not cards[0]['approvalGenerated'] and ITEM==before


def test_missing_units_remain_missing_even_when_arithmetic_suggests_thousands():
    c=run(body(unit='unit not stated'))[0]
    assert c['displayScaleCandidate']=='1000' and c['unitStatus']=='MISSING_OR_AMBIGUOUS' and 'EXPLICIT_UNIT_BINDING' in c['requiredReviews'] and not c['unitBound']


def test_conflicting_printed_unit_is_not_silently_changed_to_fit_numbers():
    c=run(body(unit='in millions'))[0];assert c['unitStatus']=='CONFLICT' and not c['unitBound'] and not c['publicationAllowed']


@pytest.mark.parametrize('old,new',[('Year Ended December 31, 2017','Three Months Ended December 31, 2017'),('Year Ended December 31, 2017','Year Ended December 30, 2017'),('846813','846812'),('4034','4035'),('Impact of ASC 606 Adoption','Unverified number'),('Net revenue','Operating expenses')])
def test_wrong_period_arithmetic_roles_or_metric_never_become_a_discovery_card(old,new):
    assert not run(body(bad=(old,new)))


def test_reverse_column_roles_are_not_accepted():
    assert not run(body(bad=('As Previously Reported','As Currently Reported')).replace(b'As Currently Reported</td></tr>',b'As Previously Reported</td></tr>'))


def test_hash_and_company_identity_are_required():
    d=body()
    with pytest.raises(ValueError,match='SOURCE_CHANGED'):discover(d,ITEM,URL,'f'*64)
    with pytest.raises(ValueError,match='IDENTITY'):discover(d,{**ITEM,'cik':'2'},URL,hashlib.sha256(d).hexdigest())


def test_split_annual_heading_and_date_still_require_actual_date():
    d=body(bad=('<td>Consolidated Statement of Operations—Year Ended December 31, 2017</td>','<td>Year Ended</td></tr><tr><td>December 31, 2017</td>'))
    c=run(d)[0];assert c['periodEndBound'] and not c['periodStartBound'] and not c['publicationAllowed']
