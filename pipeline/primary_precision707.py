"""Full primary statements linked through overlapping published precisions.

All raw source reports must participate. Rounding compatibility does not prove
unchanged economics below the displayed precision, and never edits raw facts.
"""
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
import re
from pipeline.legacy_statement_total699 import document, statement, clean, DATE
from pipeline.bound_reconciliation685 import period_years
from pipeline.reconciliation_review679 import numeric
from pipeline.reviewed_financial_mapping672 import reviewed_approval, digest
from pipeline.financial_mapping667 import invalid

BASIS='REVIEWED_PRIMARY_STATEMENT_PRECISION_CHAIN'
POLICY='sec-reviewed-primary-statement-precision707-v1'


def primary_heading(entry,rows,context,yr):
    title=r'consolidated statements of (?:income|operations|earnings)(?: \(loss\))?(?: and comprehensive (?:income|loss)(?: \(loss\))?)?'
    unit=r'(?: \([^)]{0,80}in (?:thousands|millions)[^)]{0,80}\))?'
    # A real title may be immediately before the table or inside its leading
    # header rows. Narrative references such as "as a percentage of net sales"
    # cannot satisfy the end-anchored title grammar.
    candidates=[clean(context),clean(' '.join(' '.join(r) for r in rows[:yr]))]
    for text in candidates:
        if re.search(title+unit+'$',text):return
        if str(int(entry['cik']))=='97134' and re.search(title+r' tennant company and subsidiaries'+unit+'$',text):return
        match=re.search(title+r' (years ended [a-z]+ \d{1,2},? (?:19|20)\d{2}(?:\s*,?\s*(?:and )?(?:19|20)\d{2}){1,2})'+unit+'$',text)
        if match:
            stated=re.findall(r'\b(?:19|20)\d{2}\b',match.group(1))
            if stated==period_years(rows[yr]) and len(stated)==len(set(stated)):return
    raise ValueError('PRIMARY_PRECISION_STATEMENT_TITLE')


def vector(entry,root,node):
    e=node['statement'];rows,context,_=document(e,root)
    yr,vr=e['periodRow'],e['valueRow']
    if type(yr) is not int or type(vr) is not int or not 0<=yr<vr<len(rows):
        raise ValueError('PRIMARY_PRECISION_ROWS')
    primary_heading(entry,rows,context,yr)
    header=clean(' '.join(' '.join(row) for row in rows[:vr]))
    if re.search(r'\b(?:quarter|three months|six months|nine months)\b',header):
        raise ValueError('PRIMARY_PRECISION_MIXED_DURATION')
    mode=e['periodMode']
    if mode=='DATE':
        keys=[datetime.strptime(' '.join(m.groups()),'%B %d %Y').date().isoformat() for c in rows[yr] for m in DATE.finditer(c)]
    elif mode=='YEAR':
        heading=e['annualHeading'];md=re.sub(r'^(?:for the )?years? ended ','',heading,flags=re.I).rstrip(',')
        keys=[datetime.strptime(md+', '+year,'%B %d, %Y').date().isoformat() for year in period_years(rows[yr])]
    else:raise ValueError('PRIMARY_PRECISION_EXPLICIT_PERIOD_REQUIRED')
    cells=[c for c in rows[vr][1:] if numeric(c) is not None]
    if not 2<=len(keys)<=3 or len(set(keys))!=len(keys) or len(cells)!=len(keys):
        raise ValueError('PRIMARY_PRECISION_COLUMNS')
    dates=[date.fromisoformat(day) for day in keys]
    if any(not 350<=(a-b).days<=380 for a,b in zip(dates,dates[1:])):
        raise ValueError('PRIMARY_PRECISION_ANNUAL_SPACING')
    places=node['decimalPlaces'];scale=Decimal(str(e['unitScale']))
    if type(places) is not int or not 0<=places<=3 or scale not in {Decimal(1000),Decimal(1000000)}:
        raise ValueError('PRIMARY_PRECISION_DISPLAY_CONTRACT')
    for cell in cells:
        display=cell.replace(',','').replace('$','').strip().strip('()').strip()
        if not re.fullmatch(r'\d+'+(r'\.\d{'+str(places)+'}' if places else ''),display):
            raise ValueError('PRIMARY_PRECISION_PRINTED_DECIMALS')
    amounts=[numeric(c)*scale for c in cells];result={}
    for end,amount in zip(keys,amounts):
        # Replay the unchanged full-statement annual/date/unit/label contract
        # for every comparative column, not only the target amount.
        if e.get('totalContract')=='PRIMARY_PROPERTY_PLUS_SERVICES709_V1':
            from pipeline.property_services709 import statement as component_statement
            component_statement(entry,root,e,end,amount)
        else:statement(entry,root,e,end,amount,_check_competing=False)
        if date.fromisoformat(end)>date.fromisoformat(node['filingDate']):
            raise ValueError('PRIMARY_PRECISION_FUTURE_COLUMN')
        result[end]=amount
    return result,scale/(Decimal(10)**places)


def approve(scope,entry,root):
    r=entry['review'];nodes=r['primaryPrecisionEvidence']['nodes']
    from pipeline.property_services709 import BASIS as COMPONENT_BASIS,CONTRACT, POLICY as COMPONENT_POLICY
    if r['basis'] not in {BASIS,COMPONENT_BASIS} or not 2<=len(nodes)<=6:
        raise ValueError('PRIMARY_PRECISION_CHAIN_BUDGET')
    component=r['basis']==COMPONENT_BASIS
    if component and any(n['statement'].get('totalContract')!=CONTRACT for n in nodes):raise ValueError('PROPERTY_SERVICE_ALL_STATEMENTS_REQUIRED')
    if not component and any(n['statement'].get('totalContract')==CONTRACT for n in nodes):raise ValueError('PROPERTY_SERVICE_DISTINCT_POLICY_REQUIRED')
    checked=reviewed_approval(scope,{**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD',
        'revenueStandardAdjustment':str(Decimal(r['value'])-Decimal(r['originalValue']))})
    urls=[n['statement']['url'] for n in nodes];dates=[n['filingDate'] for n in nodes]
    if len(set(urls))!=len(urls) or dates!=sorted(set(dates)) or dates[-1]!=r['filingDate']:
        raise ValueError('PRIMARY_PRECISION_REPORT_ORDER')
    if {x['source_url'] for x in checked['proof']['originalSources']}!={urls[0]} or {x['source_url'] for x in checked['proof']['sources']}!={urls[-1]}:
        raise ValueError('PRIMARY_PRECISION_ENDPOINT_REPORTS')
    if {x['source_url'] for x in scope}!=set(urls):
        raise ValueError('PRIMARY_PRECISION_ALL_RAW_REPORTS_REQUIRED')
    vectors=[vector(entry,root,n) for n in nodes];target=r['end']
    if any(target not in v for v,q in vectors):raise ValueError('PRIMARY_PRECISION_TARGET_COLUMN')
    if vectors[0][0][target]!=Decimal(r['originalValue']) or vectors[-1][0][target]!=Decimal(r['value']):
        raise ValueError('PRIMARY_PRECISION_ENDPOINT_VALUES')
    for row in scope:
        shadow=dict(row,report_date=row['period_end'])
        if row['source_tag']=='SalesRevenueNet':shadow['source_tag']='Revenues'
        if invalid(shadow,annual_only=True):raise ValueError('PRIMARY_PRECISION_SOURCE_ANNUAL')
        i=urls.index(row['source_url'])
        if str(row['filing_date'])!=dates[i] or Decimal(str(row['value']))!=vectors[i][0][target]:
            raise ValueError('PRIMARY_PRECISION_RAW_REPORT_VALUE')
    q0,qn=vectors[0][1],vectors[-1][1]
    if qn<=q0:raise ValueError('PRIMARY_PRECISION_NO_PRECISION_REDUCTION')
    def rounded(v,q):return (v/q).quantize(Decimal(1),rounding=ROUND_HALF_UP)*q
    overlaps=[]
    for (a,aq),(b,bq) in zip(vectors,vectors[1:]):
        common=sorted(set(a)&set(b))
        if len(common)<2 or bq<aq:raise ValueError('PRIMARY_PRECISION_OVERLAP_OR_PRECISION_REVERSAL')
        if any(rounded(a[k],bq)!=b[k] for k in common):
            raise ValueError('PRIMARY_PRECISION_COMPARATIVE_VALUE_CONFLICT')
        overlaps.append(common)
    if rounded(Decimal(r['originalValue']),qn)!=Decimal(r['value']):
        raise ValueError('PRIMARY_PRECISION_TARGET_ROUNDING')
    proof={**checked['proof'],'policy':COMPONENT_POLICY if component else POLICY,'review':r,'quantumsUSD':list(map(lambda v:str(v[1]),vectors)),
        'overlappingPeriodEnds':overlaps,'allSourceReportsBound':True,'originalObservationsPreserved':True,
        'accountingCause':'PUBLISHED_PRIMARY_STATEMENT_PRECISION_COMPATIBILITY',
        'meaning':'Full primary annual statements agree at the later published precision across at least two comparative periods per link. Not proof of unchanged economics below displayed precision.'}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
