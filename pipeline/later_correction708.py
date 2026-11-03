"""Later explicit calendar-year revenue corrections are review blockers.

This detector never creates an adjusted fact or silently treats a correction
as rounding. It binds all three printed columns and the exact source bytes.
"""
import gzip
import hashlib
import re
from decimal import Decimal
from pathlib import Path
from pipeline.reconciliation_review679 import Tables, numeric
from pipeline.bound_reconciliation685 import norm, period_years
from pipeline.reviewed_financial_mapping672 import digest


def corrections(body, end):
    if len(body)>32*1024**2 or not re.fullmatch(r'(?:19|20)\d{2}-12-31',end):
        return []
    p=Tables();p.feed(body.decode('utf8','replace'));found=[]
    for rows,context in zip(p.tables,p.contexts):
        c=norm(context)
        if not all(word in c for word in ['immaterial error','revenue recognition','understatement of revenue']):continue
        for yr,row in enumerate(rows[:6]):
            years=period_years(row)
            if not 1<=len(years)<=3 or len(set(years))!=len(years) or end[:4] not in years:continue
            if yr<1 or norm(' '.join(rows[yr-1]))!='year ended december 31,':continue
            for header in range(yr+1,min(yr+4,len(rows))):
                labels=[norm(x) for x in rows[header] if x.strip()]
                if labels!=['as previously reported','corrections','as corrected']*len(years):continue
                for vr in range(header+1,min(len(rows),header+10)):
                    if not rows[vr] or norm(rows[vr][0])!='total revenue':continue
                    top=norm(' '.join(' '.join(r) for r in rows[:vr]))
                    if 'in millions, except per share amounts' not in top:continue
                    amounts=[numeric(x) for x in rows[vr][1:] if numeric(x) is not None]
                    if len(amounts)!=3*len(years) or any(not x.is_finite() for x in amounts):continue
                    if any(amounts[i]+amounts[i+1]!=amounts[i+2] for i in range(0,len(amounts),3)):continue
                    i=3*years.index(end[:4]);a,d,b=(x*1000000 for x in amounts[i:i+3])
                    if d==0:continue
                    found.append(dict(tableHash=digest(rows),yearRow=yr,headerRow=header,valueRow=vr,
                        end=end,previouslyReportedUSD=str(a),correctionUSD=str(d),correctedUSD=str(b),
                        cause='EXPLICIT_LATER_PRIOR_PERIOD_REVENUE_RECOGNITION_ERROR',
                        sourceDocumentSha256=hashlib.sha256(body).hexdigest()))
    return list({digest(x):x for x in found}.values())


def validate_watchlist(bundle,root):
    blocked={}
    items=bundle.get('lateCorrectionWatchlist',[])
    if not isinstance(items,list) or len(items)>256:raise ValueError('LATE_CORRECTION_WATCHLIST_BUDGET')
    for item in items:
        if any(type(item[k]) is not int or item[k]<=0 for k in ['companyId','metricId']):raise ValueError('LATE_CORRECTION_SCOPE_IDENTITY')
        key=(item['companyId'],item['metricId'],item['end']);doc=item['document'];sha=doc['sha256']
        if key in blocked or not re.fullmatch('[a-f0-9]{64}',sha):raise ValueError('LATE_CORRECTION_WATCHLIST_IDENTITY')
        if not re.fullmatch(r'https://www\.sec\.gov/Archives/edgar/data/'+str(int(item['cik']))+r'/\d{18}/[A-Za-z0-9_.-]+',doc['url']):raise ValueError('LATE_CORRECTION_DOCUMENT_IDENTITY')
        path=Path(root)/'documents'/(sha+'.html.gz')
        if path.is_symlink():raise ValueError('LATE_CORRECTION_DOCUMENT_PATH')
        with gzip.open(path,'rb') as f:body=f.read(32*1024**2+1)
        if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=sha:raise ValueError('LATE_CORRECTION_DOCUMENT_CHANGED')
        matches=corrections(body,item['end'])
        if item['evidence'] not in matches:raise ValueError('LATE_CORRECTION_EVIDENCE_CHANGED')
        blocked[key]=item
    return blocked
