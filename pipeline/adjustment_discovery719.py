"""Bounded reported/ASC606/adjusted discovery, not an approval contract.

Missing units are explicit review requirements, never filled from numerical
coincidence. Actual period starts and accounting cause remain unverified.
"""
import hashlib,re
from datetime import datetime
from decimal import Decimal
from pipeline.reconciliation_review679 import Tables,numeric
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.legacy_statement_total699 import clean,DATE


def discover(body,item,url,expected_sha256):
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=expected_sha256:raise ValueError('ADJUSTMENT_SOURCE_CHANGED')
    prefix=f"https://www.sec.gov/Archives/edgar/data/{int(item['cik'])}/"
    if not url.startswith(prefix) or not re.fullmatch(re.escape(prefix)+r'\d{18}/[A-Za-z0-9_.-]+',url):raise ValueError('ADJUSTMENT_SOURCE_IDENTITY')
    expected=[Decimal(item[k]) for k in ('original','adjustment','chosen')]
    if expected[0]+expected[1]!=expected[2] or expected[1]==0:raise ValueError('ADJUSTMENT_EXPECTED_EQUATION')
    p=Tables();p.feed(body.decode('utf8','replace'));cards=[]
    for rows,context in zip(p.tables,p.contexts):
        text=clean(' '.join(' '.join(r) for r in rows))
        old=re.search(r'\bas (?:previously )?reported\b',text)
        change=re.search(r'\b(?:impact of asc 606 adoption|adjustment)\b',text)
        new=re.search(r'\bas (?:currently reported|adjusted)\b',text)
        if not old or not change or not new or not old.start()<change.start()<new.start():continue
        for i,row in enumerate(rows):
            if not row or not re.fullmatch(r'(?:(?:net|total|operating) )*(?:revenues?|sales)',clean(row[0])):continue
            amounts=[v for c in row[1:] if (v:=numeric(c)) is not None]
            if len(amounts)!=3 or amounts[0]+amounts[1]!=amounts[2]:continue
            scales=[s for s in (Decimal(1),Decimal(1000),Decimal(1000000)) if [v*s for v in amounts]==expected]
            if len(scales)!=1:continue
            # The closest explicit date heading wins, never the first period
            # in a multi-section table or an unrelated preceding document.
            heading=None
            for j in range(i-1,-1,-1):
                line=' '.join(rows[j]);dates=list(DATE.finditer(line))
                if not dates:continue
                if len(dates)!=1:break
                annual=bool(re.search(r'\byear ended\b',clean(line)))
                if not annual:
                    before=next((clean(' '.join(r)) for r in reversed(rows[:j]) if clean(' '.join(r))),None)
                    annual=before=='year ended' and clean(line)==clean(dates[0].group())
                if annual:
                    end=datetime.strptime(' '.join(dates[0].groups()),'%B %d %Y').date().isoformat()
                    if end==item['end']:heading=j
                break
            if heading is None:continue
            unit_text=context+' '+' '.join(' '.join(r) for r in rows[:i+1])
            units=set(re.findall(r'\bin (thousands|millions)\b',clean(unit_text)))
            scale=scales[0];bound_scale=Decimal(1000) if units=={'thousands'} else Decimal(1000000) if units=={'millions'} else None
            unit_status='BOUND' if bound_scale==scale else 'MISSING_OR_AMBIGUOUS' if bound_scale is None else 'CONFLICT'
            cards.append(dict(documentSha256=expected_sha256,tableHash=digest(rows),url=url,valueRow=i,periodHeadingRow=heading,rowCells=row,periodEnd=item['end'],periodEndBound=True,periodStartBound=False,displayScaleCandidate=str(scale),unitStatus=unit_status,unitBound=unit_status=='BOUND',requiredReviews=['ACTUAL_PERIOD_START','STATEMENT_ENTITY','ACCOUNTING_CAUSE','ALL_SOURCE_REPORTS']+([] if unit_status=='BOUND' else ['EXPLICIT_UNIT_BINDING']),numericEquationMatches=True,approvalGenerated=False,publicationAllowed=False))
    return cards
