"""Ford 2013 automotive component versus consolidated total revenue.

Only this period is registered. Earlier financial-services comparative totals
change between reports and are deliberately outside this contract.
"""
import re
from decimal import Decimal
from pipeline.legacy_statement_total699 import clean,document
from pipeline.reviewed_financial_mapping672 import reviewed_approval,digest
from pipeline.original_sec_scope725 import verify

BASIS='REVIEWED_FORD_AUTOMOTIVE_FINANCIAL_TOTAL734'
POLICY='sec-ford-automotive-financial-total734-v1'
RAW_SHA256='a572be781d554ea39f47c7b478d31b137f91b87d769f044ce2cb922ac7d36189'

def metadata(r):
    if (r['companyId'],r['metricId'],r['metric'],r['start'],r['end'],r['chosenTag'])!=(145,22,'REVENUE','2013-01-01','2013-12-31','Revenues'):
        raise ValueError('FORD_ONLY_2013_CONSOLIDATED_TOTAL734')

def vector(rows,context):
    if not clean(context).endswith('ford motor company and subsidiaries consolidated income statement (in millions, except per share amounts)'):
        raise ValueError('FORD_FULL_CONSOLIDATED_PRIMARY_UNIT734')
    nonempty=[(i,r) for i,r in enumerate(rows) if any(c.strip() for c in r)]
    if len(nonempty)<10 or clean(' '.join(nonempty[0][1]))!='for the years ended december 31,':raise ValueError('FORD_ACTUAL_CALENDAR_ANNUAL734')
    cells=[c.strip() for c in nonempty[1][1] if c.strip()]
    if len(cells)!=3 or not all(re.fullmatch(r'20\d{2}',c) for c in cells):raise ValueError('FORD_THREE_PRIMARY_YEAR_COLUMNS734')
    years=list(map(int,cells))
    if years[0] not in {2013,2014,2015} or years!=list(range(years[0],years[0]-3,-1)):raise ValueError('FORD_REGISTERED_PRIMARY_YEARS734')
    block=[r for _,r in nonempty[2:6]]
    if [clean(r[0]) for r in block]!=['revenues','automotive','financial services','total revenues'] or any(c.strip() for c in block[0][1:]):raise ValueError('FORD_COMPLETE_REVENUE_PARENT_CHILD734')
    def values(row):
        cells=[c.strip() for c in row[1:] if c.strip() and c.strip()!='$']
        if len(cells)!=3 or any(not re.fullmatch(r'\d{1,3}(?:,\d{3})*',c) for c in cells):raise ValueError('FORD_LITERAL_MILLION_REVENUE734')
        return [Decimal(c.replace(',',''))*1000000 for c in cells]
    if block[1].count('$')!=3:raise ValueError('FORD_PRIMARY_DOLLAR_MARK734')
    automotive,financial,totals=[values(r) for r in block[1:]]
    if any(a+b!=t for a,b,t in zip(automotive,financial,totals)):raise ValueError('FORD_ALL_REVENUE_COMPONENT_EQUATIONS734')
    if not any(r and clean(r[0])=='costs and expenses' for r in rows) or not any(r and clean(r[0]).startswith('net income') for r in rows):raise ValueError('FORD_FULL_INCOME_STATEMENT_HIERARCHY734')
    return {str(y)+'-12-31':dict(automotive=a,financial=f,total=t) for y,a,f,t in zip(years,automotive,financial,totals)}

def approve(scope,entry,root):
    r=entry['review'];metadata(r)
    if r['basis']!=BASIS or str(int(entry['cik']))!='37996':raise ValueError('FORD_BASIS_COMPANY734')
    checked=reviewed_approval(scope,{**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD','revenueStandardAdjustment':str(Decimal(r['value'])-Decimal(r['originalValue']))})
    raw=verify(scope,r['originalRawEvidence'],root,company=145,cik='0000037996',raw_sha256=RAW_SHA256,metric='REVENUE',tags={'SalesRevenueNet','Revenues'})
    nodes=r['consolidatedTotalEvidence']['nodes'];urls=[n['statement']['url'] for n in nodes];dates=[n['filingDate'] for n in nodes]
    if len(nodes)!=3 or len(set(urls))!=3 or dates!=sorted(set(dates)) or set(urls)!={x['source_url'] for x in scope} or dates[-1]!=r['filingDate']:raise ValueError('FORD_ALL_SOURCE_REPORTS734')
    if {x['source_url'] for x in checked['proof']['originalSources']}!={urls[0]} or {x['source_url'] for x in checked['proof']['sources']}!={urls[-1]}:raise ValueError('FORD_PRIMARY_ENDPOINTS734')
    vectors=[]
    for n in nodes:
        e=n['statement']
        if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:raise ValueError('FORD_DOCUMENT_BINDING734')
        rows,ctx,_=document(e,root);v=vector(rows,ctx)
        if max(v)>n['filingDate'] or r['end'] not in v:raise ValueError('FORD_ACTUAL_PRIMARY_PERIOD734')
        vectors.append(v)
    for a,b in zip(vectors,vectors[1:]):
        common=set(a)&set(b)
        if len(common)!=2 or any(a[k]!=b[k] for k in common):raise ValueError('FORD_ALL_COMPARATIVE_COMPONENTS_UNCHANGED734')
    counts={'SalesRevenueNet':0,'Revenues':0}
    for row in scope:
        i=urls.index(row['source_url']);v=vectors[i];tag=row['source_tag'];key='automotive' if tag=='SalesRevenueNet' else 'total'
        if tag not in counts or row['form_type'] not in {'10-K','10-K/A'} or str(row['filing_date'])!=dates[i] or str(row['report_date'])!=max(v) or Decimal(str(row['value']))!=v[r['end']][key]:raise ValueError('FORD_EVERY_AUTOMOTIVE_TOTAL_RAW_OBSERVATION734')
        counts[tag]+=1
    if set(counts.values())!={3}:raise ValueError('FORD_COMPLETE_BOTH_REVENUE_TAGS734')
    proof={**checked['proof'],'policy':POLICY,'review':r,'accountingCause':'EXPLICIT_AUTOMOTIVE_COMPONENT_VS_CONSOLIDATED_TOTAL','originalRawEvidence':raw,
           'allSourceReportsBound':True,'originalObservationsPreserved':True,'restatementClaimed':False,
           'financialServicesComponentUSD':str(vectors[0][r['end']]['financial']),
           'meaning':'Only Ford 2013 revenue. Automotive plus Financial Services equals the primary consolidated total in every source and comparative column. Earlier years and EPS changes are not approved; not a restatement or organic-growth claim.'}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
