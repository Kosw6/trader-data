from copy import deepcopy
import pytest
from pipeline.discontinued_review743 import diagnose


def values(year):
    if year==2012:
        return dict(operating='48392000',pretax='48234000',tax='18577000',net='29657000'),dict(operating='46957000',pretax='46799000',tax='18056000',net='28743000'),dict(afterTaxIncome='914000',tax='522000',revenue='26452000')
    return dict(operating='49190000',pretax='50732000',tax='8656000',net='42076000'),dict(operating='45584000',pretax='47126000',tax='7387000',net='39739000'),dict(afterTaxIncome='2337000',tax='1269000',revenue='33905000')


def test_literal_2012_tax_conflict_is_not_rounded_away():
    a,b,n=values(2012);before=deepcopy((a,b,n));r=diagnose(295,2012,a,b,n,unit='USD')
    assert r['status']=='PRIMARY_VS_NOTE_TAX_ALLOCATION_CONFLICT' and r['primaryVsNoteTaxDifferenceUSD']=='-1000'
    assert r['operatingChangeUSD']=='1435000' and r['printedNotePretaxUSD']=='1436000'
    assert not r['amountRoundedOrAdjusted'] and not r['publicationAllowed'] and not r['approvalGenerated'] and (a,b,n)==before


def test_exact_2013_still_requires_original_sources_and_fresh_database():
    r=diagnose(295,2013,*values(2013),unit='USD')
    assert r['status']=='EXACT_PRIMARY_AND_NOTE_BRIDGE_REVIEW_ONLY' and r['operatingChangeUSD']=='3606000'
    assert not r['publicationAllowed'] and not r['approvalGenerated'] and 'FRESH_DATABASE_AND_SERVING_VALIDATION' in r['requires']


@pytest.mark.parametrize('kind',['other_company','other_year','wrong_unit','missing_primary','missing_note','nan','infinite','wrong_net','wrong_tax','wrong_allocation'])
def test_incomplete_or_mixed_scope_never_becomes_a_review_result(kind):
    a,b,n=values(2013);cid=295;year=2013;unit='USD'
    if kind=='other_company':cid=1
    if kind=='other_year':year=2014
    if kind=='wrong_unit':unit='USD/shares'
    if kind=='missing_primary':a.pop('tax')
    if kind=='missing_note':n.pop('tax')
    if kind=='nan':a['tax']='NaN'
    if kind=='infinite':b['pretax']='Infinity'
    if kind=='wrong_net':a['net']='42076001'
    if kind=='wrong_tax':b['tax']='7387001'
    if kind=='wrong_allocation':n['afterTaxIncome']='2337001'
    with pytest.raises(ValueError):diagnose(cid,year,a,b,n,unit=unit)


def test_even_a_mathematically_exact_altered_note_cannot_issue_approval():
    a,b,n=values(2012);n['tax']='521000';r=diagnose(295,2012,a,b,n,unit='USD')
    assert r['status']=='EXACT_PRIMARY_AND_NOTE_BRIDGE_REVIEW_ONLY' and not r['publicationAllowed'] and not r['approvalGenerated']
