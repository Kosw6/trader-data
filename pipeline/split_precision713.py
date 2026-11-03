"""Exact split month/day + year headers in reviewed UCTT primary statements.

This contract never infers a calendar year end, edits raw facts, or accepts a
rounded note/segment/pro-forma table as the full consolidated statement.
"""
import re
from datetime import datetime,date
from decimal import Decimal,ROUND_HALF_UP
from pipeline.legacy_statement_total699 import document,clean
from pipeline.primary_precision707 import primary_heading
from pipeline.reconciliation_review679 import numeric
from pipeline.reviewed_financial_mapping672 import reviewed_approval,digest
from pipeline.financial_mapping667 import invalid

BASIS='REVIEWED_SPLIT_DATE_PRIMARY_PRECISION_CHAIN713'
POLICY='sec-reviewed-split-date-primary-precision713-v1'
CONTRACT='UCTT_SPLIT_MONTH_DAY_YEAR713_V1'


def vector(entry,root,node):
    e=node['statement']
    if str(int(entry['cik']))!='1275014' or e.get('totalContract')!=CONTRACT or e.get('periodMode')!='SPLIT_MONTH_DAY_YEAR713':
        raise ValueError('SPLIT_PRIMARY_COMPANY_OR_CONTRACT')
    if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:
        raise ValueError('SPLIT_PRIMARY_DOCUMENT_IDENTITY')
    rows,context,_=document(e,root)
    if e.get('periodRow')!=2 or e.get('monthDayRow')!=1 or e.get('unitRow')!=3 or e['valueRow'] not in {4,7} or len(rows)<14:
        raise ValueError('SPLIT_PRIMARY_BOUND_ROWS')
    primary_heading(entry,rows,context,2)
    if [clean(c) for c in rows[0] if clean(c)]!=['fiscal year ended']:
        raise ValueError('SPLIT_PRIMARY_ANNUAL_HEADING')
    md={i:clean(c) for i,c in enumerate(rows[1]) if clean(c)}
    years={i:clean(c) for i,c in enumerate(rows[2]) if clean(c)}
    if len(md)!=3 or set(md)!=set(years) or any(not re.fullmatch(r'december \d{1,2},',v) for v in md.values()) or any(not re.fullmatch(r'(?:19|20)\d{2}',v) for v in years.values()):
        raise ValueError('SPLIT_PRIMARY_COLUMN_ALIGNMENT')
    keys=[datetime.strptime(md[i]+' '+years[i],'%B %d, %Y').date().isoformat() for i in sorted(md)]
    dates=list(map(date.fromisoformat,keys))
    if len(set(keys))!=3 or any(not 350<=(a-b).days<=380 for a,b in zip(dates,dates[1:])):
        raise ValueError('SPLIT_PRIMARY_ANNUAL_SPACING')
    units=[clean(c) for c in rows[3] if clean(c)]
    if len(units)!=1 or units[0] not in {'(in thousands, except per share amounts)','(in millions, except per share amounts)'}:
        raise ValueError('SPLIT_PRIMARY_EXACT_UNIT_HEADER')
    scale=Decimal(1000 if 'thousands' in units[0] else 1000000)
    places=node['decimalPlaces']
    if Decimal(str(e['unitScale']))!=scale or type(places) is not int or places!=(0 if scale==1000 else 1):
        raise ValueError('SPLIT_PRIMARY_DISPLAY_PRECISION')
    def amounts(row,allow_dash=False):
        if any('%' in c for c in row):raise ValueError('SPLIT_PRIMARY_PERCENT_NOT_AMOUNT')
        cells=[c for c in row[1:] if numeric(c) is not None or (allow_dash and c.strip()=='-')]
        if len(cells)!=3 or any(not (allow_dash and c.strip()=='-') and not re.fullmatch(r'\d+(?:,\d{3})*'+(r'\.\d' if places else ''),c.strip()) for c in cells):
            raise ValueError('SPLIT_PRIMARY_AMOUNT_COLUMNS')
        return [Decimal(0) if c.strip()=='-' else numeric(c)*scale for c in cells]
    vr=e['valueRow'];label=clean(rows[vr][0])
    if label!=clean(e['rowLabel']):raise ValueError('SPLIT_PRIMARY_BOUND_LABEL')
    if vr==4:
        if label not in {'sales','revenues'}:raise ValueError('SPLIT_PRIMARY_FULL_REVENUE_LABEL')
        values=amounts(rows[vr])
    else:
        if [clean(rows[i][0]) for i in range(4,8)]!=['revenues:','product','services','total revenues']:
            raise ValueError('SPLIT_PRIMARY_COMPONENT_HIERARCHY')
        if any(numeric(c) is not None for c in rows[4][1:]):raise ValueError('SPLIT_PRIMARY_COMPONENT_HEADING')
        a,b,values=amounts(rows[5]),amounts(rows[6],allow_dash=True),amounts(rows[7])
        if any(x+y!=z for x,y,z in zip(a,b,values)):raise ValueError('SPLIT_PRIMARY_ALL_COMPONENT_EQUATIONS')
    if any(d>date.fromisoformat(node['filingDate']) for d in dates):raise ValueError('SPLIT_PRIMARY_FUTURE_COLUMN')
    return dict(zip(keys,values)),scale/(Decimal(10)**places)


def approve(scope,entry,root):
    r=entry['review'];nodes=r['primaryPrecisionEvidence']['nodes']
    if r['basis']!=BASIS or r['companyId']!=1942 or r['metric']!='REVENUE' or len(nodes)!=3:
        raise ValueError('SPLIT_PRIMARY_REVIEW_SCOPE')
    checked=reviewed_approval(scope,{**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD','revenueStandardAdjustment':str(Decimal(r['value'])-Decimal(r['originalValue']))})
    urls=[n['statement']['url'] for n in nodes];dates=[n['filingDate'] for n in nodes]
    if len(set(urls))!=3 or dates!=sorted(set(dates)) or dates[-1]!=r['filingDate'] or {x['source_url'] for x in scope}!=set(urls):
        raise ValueError('SPLIT_PRIMARY_ALL_REPORTS_IN_ORDER')
    if {x['source_url'] for x in checked['proof']['originalSources']}!={urls[0]} or {x['source_url'] for x in checked['proof']['sources']}!={urls[-1]}:
        raise ValueError('SPLIT_PRIMARY_ENDPOINT_REPORTS')
    vectors=[vector(entry,root,n) for n in nodes];target=r['end']
    if any(target not in v for v,q in vectors) or vectors[0][0][target]!=Decimal(r['originalValue']) or vectors[-1][0][target]!=Decimal(r['value']):
        raise ValueError('SPLIT_PRIMARY_ENDPOINT_VALUES')
    for row in scope:
        shadow=dict(row,report_date=row['period_end'])
        if row['source_tag']=='SalesRevenueNet':shadow['source_tag']='Revenues'
        if invalid(shadow,annual_only=True):raise ValueError('SPLIT_PRIMARY_SOURCE_ANNUAL')
        i=urls.index(row['source_url'])
        if str(row['filing_date'])!=dates[i] or Decimal(str(row['value']))!=vectors[i][0][target]:raise ValueError('SPLIT_PRIMARY_RAW_REPORT_VALUE')
    if vectors[-1][1]<=vectors[0][1]:raise ValueError('SPLIT_PRIMARY_NO_PRECISION_REDUCTION')
    def rounded(v,q):return (v/q).quantize(Decimal(1),rounding=ROUND_HALF_UP)*q
    overlaps=[]
    for (a,aq),(b,bq) in zip(vectors,vectors[1:]):
        common=sorted(set(a)&set(b))
        if len(common)<2 or bq<aq or any(rounded(a[k],bq)!=b[k] for k in common):raise ValueError('SPLIT_PRIMARY_COMPARATIVE_CONFLICT')
        overlaps.append(common)
    proof={**checked['proof'],'policy':POLICY,'review':r,'quantumsUSD':[str(q) for v,q in vectors],'overlappingPeriodEnds':overlaps,'allSourceReportsBound':True,'originalObservationsPreserved':True,'accountingCause':'PUBLISHED_PRIMARY_STATEMENT_PRECISION_COMPATIBILITY','meaning':'Exact split header dates and all comparative values agree at later displayed precision; not proof of unchanged economics below that precision.'}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
