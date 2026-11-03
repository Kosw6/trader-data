"""Review arithmetic across all overlapping periods, not a target-only bridge."""
from datetime import date
from decimal import Decimal


def review(original,continuing,discontinued,*,target,printed_quantum):
    maps=[]
    for source in (original,continuing,discontinued):
        if not isinstance(source,dict) or not source:raise ValueError('COMPARATIVE_SOURCE_VECTOR_REQUIRED')
        current={}
        for end,value in source.items():
            date.fromisoformat(end);v=Decimal(str(value))
            if not v.is_finite() or v<0:raise ValueError('COMPARATIVE_INVALID_REVENUE')
            current[end]=v
        maps.append(current)
    q=Decimal(str(printed_quantum))
    if not q.is_finite() or q<=0:raise ValueError('COMPARATIVE_PRINTED_QUANTUM')
    original,continuing,discontinued=maps;common=sorted(set(original)&set(continuing)&set(discontinued))
    if target not in common or len(common)<2:raise ValueError('COMPARATIVE_TWO_OVERLAPPING_PERIODS_REQUIRED')
    rows=[]
    for end in common:
        residual=original[end]-continuing[end]-discontinued[end]
        rows.append(dict(end=end,previous=str(original[end]),continuing=str(continuing[end]),discontinued=str(discontinued[end]),residual=str(residual),residualInPrintedUnits=str(residual/q),exactEquation=residual==0))
    exact=all(r['exactEquation'] for r in rows)
    return dict(target=target,targetEquationExact=next(r['exactEquation'] for r in rows if r['end']==target),allComparativeEquationsExact=exact,reasonCode='EXACT_COMPARATIVE_ARITHMETIC_REQUIRES_SOURCE_AND_DB_REVIEW' if exact else 'COMPARATIVE_DISPLAY_DIFFERENCE_REVIEW_REQUIRED',rows=rows,implicitRoundingToleranceApplied=False,approvalGenerated=False,publicationAllowed=False,meaning='A one-period numerical match does not establish the complete accounting representation. Display-unit differences require source/precision review; they are not silently rounded away or called raw-data errors.')
