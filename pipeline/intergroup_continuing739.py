"""One continuing-revenue total; never add discontinued-operation revenue.

The segment Total includes a separately labelled discontinued column. The
statement's continuing total is preserved, not overwritten by that aggregate.
"""
import re
from decimal import Decimal
from pipeline.legacy_statement_total699 import clean,document
from pipeline.reviewed_financial_mapping672 import reviewed_approval,digest
from pipeline.original_sec_scope725 import verify

BASIS='REVIEWED_INTERGROUP_CONTINUING_SEGMENT_TOTAL739'
POLICY='sec-intergroup-continuing-segment-total739-v1'
RAW_SHA256='13bb3ebcbaa7f0b37deca80e4b994f5a144306db451d17c1802fe5df3e0684b5'


def metadata(r):
    if (r['companyId'],r['metricId'],r['metric'],r['start'],r['end'],r['chosenTag'])!=(244,22,'REVENUE','2011-07-01','2012-06-30','SalesRevenueNet'):
        raise ValueError('INTERGROUP_ONLY_NAMED_CONTINUING_REVENUE739')


def values(cells,expected,zero=False):
    cells=[c.strip() for c in cells if c.strip() and c.strip()!='$'];result=[];i=0
    while i<len(cells):
        s=cells[i]
        if s.startswith('(') and not s.endswith(')') and i+1<len(cells) and cells[i+1]==')':s+=')';i+=1
        if zero and s=='-':result.append(Decimal(0))
        elif re.fullmatch(r'\(?\d{1,3}(?:,\d{3})*\)?',s) and s.startswith('(')==s.endswith(')'):result.append(Decimal(s.strip('()').replace(',',''))*(-1 if s.startswith('(') else 1))
        else:raise ValueError('INTERGROUP_LITERAL_DOLLAR_CELL739')
        i+=1
    if len(result)!=expected:raise ValueError('INTERGROUP_EXACT_VALUE_COLUMNS739')
    return result


def years(row):
    h=[c.strip() for c in row if c.strip()]
    if len(h) not in {2,3} or clean(h[0])!='for the years ended june 30,' or any(not re.fullmatch(r'20\d{2}',c) for c in h[1:]):raise ValueError('INTERGROUP_EXPLICIT_ANNUAL_DATE739')
    y=list(map(int,h[1:]))
    if y not in [[2012,2011],[2013,2012],[2012]]:raise ValueError('INTERGROUP_NAMED_ANNUAL_YEARS739')
    return [str(v)+'-06-30' for v in y]


def primary(rows,context):
    if not clean(context).endswith('the intergroup corporation consolidated statements of operations'):raise ValueError('INTERGROUP_FULL_PRIMARY_CONTEXT739')
    dates=years(rows[0]);n=len(dates)
    if n!=2 or [clean(r[0]) for r in rows[1:6]]!=['revenues:','hotel','real estate','total revenues','costs and operating expenses:'] or rows[2].count('$')!=2:raise ValueError('INTERGROUP_PRIMARY_REVENUE_HIERARCHY739')
    hotel,estate,total=[values(r[1:],n) for r in rows[2:5]]
    def line(label):
        hits=[r for r in rows[6:] if r and clean(r[0])==label]
        if len(hits)!=1:raise ValueError('INTERGROUP_PRIMARY_FULL_COST_HIERARCHY739')
        return values(hits[0][1:],n)
    costs=[line(l) for l in ['hotel operating expenses','real estate operating expenses','depreciation and amortization expense','general and administrative expense']];ct=line('total costs and operating expenses');op=line('income from operations')
    if any(a+b!=t or sum(v[i] for v in costs)!=ct[i] or t+ct[i]!=op[i] for i,(a,b,t) in enumerate(zip(hotel,estate,total))):raise ValueError('INTERGROUP_ALL_PRIMARY_COMPONENT_EQUATIONS739')
    return {d:dict(hotel=a,estate=b,total=t) for d,a,b,t in zip(dates,hotel,estate,total)}


def discontinued(rows,context):
    ctx=clean(context)
    if 'reported as income from discontinued operations in the consolidated statements of operations for the respective period' not in ctx or not re.search(r'revenues and expenses .* (?:properties|property) that (?:were|was) sold',ctx):raise ValueError('INTERGROUP_EXPLICIT_DISCONTINUED_CAUSE739')
    dates=years(rows[0]);n=len(dates)
    if len(rows)!=4 or [clean(r[0]) for r in rows[1:]]!=['revenues','expenses','income from discontinued operations'] or rows[1].count('$')!=n or rows[3].count('$')!=n:raise ValueError('INTERGROUP_DISCONTINUED_TABLE_HIERARCHY739')
    rev,expense,income=[values(r[1:],n) for r in rows[1:]]
    if any(a+b!=c or b>=0 for a,b,c in zip(rev,expense,income)):raise ValueError('INTERGROUP_ALL_DISCONTINUED_EQUATIONS739')
    return dict(zip(dates,rev))


def segment(rows,context):
    if 'segment information' not in clean(context) or 'reportable segments' not in clean(context):raise ValueError('INTERGROUP_SEGMENT_CONTEXT739')
    result={}
    for i,row in enumerate(rows):
        if not row or clean(row[0])!='as of and for the year':continue
        if i+2>=len(rows):raise ValueError('INTERGROUP_SEGMENT_BLOCK739')
        top=[clean(c) for c in row[1:] if c.strip()];h=[clean(c) for c in rows[i+1] if c.strip()]
        if top!=['hotel','real estate','investment','discontinued'] or len(h)!=8 or not re.fullmatch(r'ended june 30, 201[23]',h[0]) or h[1:]!=['operations','operations','transactions','other','subtotal','operations','total'] or clean(rows[i+2][0])!='revenues':raise ValueError('INTERGROUP_CONTINUING_DISCONTINUED_COLUMNS739')
        y=h[0][-4:]+'-06-30'
        if y in result or rows[i+2].count('$')!=7:raise ValueError('INTERGROUP_SEGMENT_UNIQUE_DOLLAR_YEAR739')
        a,b,c,d,sub,dco,total=values(rows[i+2][1:],7,zero=True)
        if c or d or a+b!=sub or sub+dco!=total or min(a,b,sub,dco,total)<0:raise ValueError('INTERGROUP_SEGMENT_PARENT_EQUATIONS739')
        result[y]=dict(hotel=a,estate=b,total=sub,discontinued=dco,includingDiscontinued=total)
    if list(result) not in [['2012-06-30'],['2013-06-30','2012-06-30']]:raise ValueError('INTERGROUP_SEGMENT_ALL_NAMED_BLOCKS739')
    return result


def approve(scope,entry,root):
    r=entry['review'];metadata(r)
    if r['basis']!=BASIS or str(int(entry['cik']))!='69422':raise ValueError('INTERGROUP_BASIS_CIK739')
    checked=reviewed_approval(scope,{**r,'basis':'UNCHANGED_TOTAL_CONFIRMED'},_reviewed_legacy_total=True)
    raw=verify(scope,r['originalRawEvidence'],root,company=244,cik='0000069422',raw_sha256=RAW_SHA256,metric='REVENUE',tags={'SalesRevenueNet','Revenues'})
    nodes=r['continuingRevenueEvidence']['nodes'];urls=[n['statement']['url'] for n in nodes];dates=[n['filingDate'] for n in nodes]
    if len(nodes)!=2 or len(set(urls))!=2 or dates!=sorted(set(dates)) or dates[-1]!=r['filingDate'] or set(urls)!={x['source_url'] for x in scope}:raise ValueError('INTERGROUP_ALL_SOURCE_REPORTS739')
    vectors=[]
    for n in nodes:
        p,s,d=[n[k] for k in ['statement','segments','discontinued']]
        if (p['url'],p['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']} or any((x['url'],x['sha256'])!=(p['url'],p['sha256']) for x in [s,d]):raise ValueError('INTERGROUP_SAME_REPORT_BINDINGS739')
        rows,ctx,_=document(p,root);v=primary(rows,ctx);rows,ctx,_=document(s,root);seg=segment(rows,ctx);rows,ctx,_=document(d,root);dco=discontinued(rows,ctx)
        if r['end'] not in v or max(v)>n['filingDate'] or v[r['end']]['total']!=Decimal(r['value']):raise ValueError('INTERGROUP_PRIMARY_TARGET_PERIOD739')
        if any(day not in v or {k:z[k] for k in ['hotel','estate','total']}!=v[day] or (z['discontinued']!=dco.get(day,Decimal(0))) for day,z in seg.items()):raise ValueError('INTERGROUP_SEGMENT_PRIMARY_DCO_BRIDGE739')
        if dco.get(r['end'])!=Decimal(208000) or seg[r['end']]['includingDiscontinued']!=Decimal(57207000):raise ValueError('INTERGROUP_NAMED_REVENUE_DIFFERENCE739')
        vectors.append((v,seg,dco))
    a,b=vectors
    if a[0][r['end']]!=b[0][r['end']] or a[1][r['end']]!=b[1][r['end']] or a[2][r['end']]!=b[2][r['end']]:raise ValueError('INTERGROUP_ALL_REVENUE_COMPARATIVES739')
    counts={'SalesRevenueNet':0,'Revenues':0}
    for row in scope:
        i=urls.index(row['source_url']);tag=row['source_tag'];v,seg,dco=vectors[i]
        if tag not in counts or str(row['filing_date'])!=dates[i] or str(row['report_date'])!=max(v) or row['form_type'] not in {'10-K','10-K/A'}:raise ValueError('INTERGROUP_RAW_REPORT_IDENTITY739')
        expected=v[r['end']]['total'] if tag=='SalesRevenueNet' else seg[r['end']]['includingDiscontinued']
        if Decimal(str(row['value']))!=expected:raise ValueError('INTERGROUP_EVERY_RAW_TAG_MEANING739')
        counts[tag]+=1
    if set(counts.values())!={2}:raise ValueError('INTERGROUP_COMPLETE_BOTH_TAG_OBSERVATIONS739')
    proof={**checked['proof'],'policy':POLICY,'review':r,'originalRawEvidence':raw,'accountingCause':'SEGMENT_TOTAL_INCLUDES_SEPARATELY_PRESENTED_DISCONTINUED_REVENUE','allSourceReportsBound':True,'originalObservationsPreserved':True,'restatementClaimed':False,'discontinuedRevenueUSD':'208000','servingTagContract':'EXPLICIT_COMPANY_PERIOD_LEGACY_STATEMENT_TOTAL699_V1','mappingScope':'COMPANY_METRIC_PERIOD_END_ONLY','meaning':'Only InterGroup FY2012 continuing-operation revenue. Hotel plus real estate equals the unchanged primary total; the segment including-discontinued total is not substituted. Segment expense presentation changes and other metrics are not approved.'}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
