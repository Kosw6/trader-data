from copy import deepcopy
import pytest
from pipeline.comparative_bridge721 import review


def inputs():return ({'2011-12-31':'6022000000','2010-12-31':'7167000000'},{'2011-12-31':'5148000000','2010-12-31':'5993000000'},{'2011-12-31':'874000000','2010-12-31':'1173000000'})


def test_target_match_cannot_hide_the_adjacent_year_display_difference():
    data=inputs();before=deepcopy(data);r=review(*data,target='2011-12-31',printed_quantum='1000000')
    assert data==before and r['targetEquationExact'] and not r['allComparativeEquationsExact']
    assert r['rows'][0]['residual']=='1000000' and r['rows'][0]['residualInPrintedUnits']=='1'
    assert not r['implicitRoundingToleranceApplied'] and not r['publicationAllowed'] and not r['approvalGenerated']


def test_exact_arithmetic_still_does_not_approve_without_accounting_and_db_review():
    a,b,c=inputs();a['2010-12-31']='7166000000';r=review(a,b,c,target='2011-12-31',printed_quantum='1000000')
    assert r['allComparativeEquationsExact'] and not r['publicationAllowed']


@pytest.mark.parametrize('value',['NaN','Infinity','-1'])
def test_nonfinite_or_negative_total_is_not_a_review_vector(value):
    a,b,c=inputs();a['2010-12-31']=value
    with pytest.raises(ValueError):review(a,b,c,target='2011-12-31',printed_quantum='1000000')


@pytest.mark.parametrize('quantum',['0','-1','NaN','Infinity'])
def test_invalid_unit_quantum_does_not_enable_a_tolerance(quantum):
    with pytest.raises(ValueError):review(*inputs(),target='2011-12-31',printed_quantum=quantum)


def test_missing_comparative_or_actual_date_is_not_filled():
    a,b,c=inputs();c.pop('2010-12-31')
    with pytest.raises(ValueError):review(a,b,c,target='2011-12-31',printed_quantum='1000000')
    a['2010-13-31']='1'
    with pytest.raises(ValueError):review(a,b,c,target='2011-12-31',printed_quantum='1000000')
