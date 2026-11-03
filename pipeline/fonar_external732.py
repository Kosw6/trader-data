"""Fonar's two named consolidated totals versus intersegment revenue tags.

Every full primary component and external-customer segment sum is required.
Intersegment sales are kept as a distinct original observation, not promoted
as consolidated net revenue and not added to the external-customer total.
"""
import re
from decimal import Decimal
from pipeline.legacy_statement_total699 import clean,document
from pipeline.reviewed_financial_mapping672 import reviewed_approval,digest
from pipeline.original_sec_scope725 import verify

BASIS='REVIEWED_FONAR_EXTERNAL_TOTAL_VS_INTERSEGMENT732'
POLICY='sec-fonar-external-total-intersegment732-v1'
RAW_SHA256='a67f92d64c565307bdfb231b1f65a026ffcf19a3099aa5903108b162dba62262'
PERIODS={('2011-07-01','2012-06-30'),('2012-07-01','2013-06-30')}

def metadata(r):
    if (r['companyId'],r['metricId'],r['metric'],r['chosenTag'])!=(478,22,'REVENUE','SalesRevenueNet') or (r['start'],r['end']) not in PERIODS:raise ValueError('FONAR_NAMED_EXTERNAL_TOTAL732')

def label(text):return re.sub(r'\s*[\u2013\u2014-]\s*',' ',clean(text))

def amounts(cells,*,printed_zero=False):
    result=[];i=0
    while i<len(cells):
        c=cells[i].strip();i+=1
        if c in {'','$'}:continue
        if c=='\u2014' and printed_zero:result.append(Decimal(0));continue
        if c.startswith('(') and not c.endswith(')'):
            while i<len(cells) and not cells[i].strip():i+=1
            if i==len(cells) or cells[i].strip()!=')':raise ValueError('FONAR_NEGATIVE_CELL_CLOSURE732')
            c+=')';i+=1
        if not re.fullmatch(r'\(?\d{1,3}(?:,\d{3})*\)?',c) or c.startswith('(')!=c.endswith(')'):raise ValueError('FONAR_LITERAL_DOLLAR_CELL732')
        result.append(Decimal(c.strip('()').replace(',',''))*(-1 if c.startswith('(') else 1))
    return result

def primary(rows,context):
    if not clean(context).endswith('fonar corporation and subsidiaries consolidated statements of income'):raise ValueError('FONAR_FULL_PRIMARY_TITLE732')
    heads=[]
    for i,row in enumerate(rows[:3]):
        cells=[c.strip() for c in row if c.strip()]
        if not cells:continue
        heads.append((i,cells))
        if all(re.fullmatch(r'20\d{2}',c) for c in cells):break
    if len(heads)!=2 or clean(' '.join(heads[0][1]))!='for the years ended june 30,' or not all(re.fullmatch(r'20\d{2}',c) for c in heads[1][1]):raise ValueError('FONAR_ACTUAL_JUNE30_ANNUAL732')
    yr=heads[1][0];years=list(map(int,heads[1][1]));expected=[2012,2011] if years[0]==2012 else [2013,2012] if years[0]==2013 else [2014,2013] if years[0]==2014 else [2015,2014,2013]
    if years!=expected:raise ValueError('FONAR_EXPLICIT_PRIMARY_YEAR_COLUMNS732')
    nonempty=[(i,row) for i,row in enumerate(rows[yr+1:],start=yr+1) if any(c.strip() for c in row)]
    if not nonempty or label(nonempty[0][1][0])!='revenues' or amounts(nonempty[0][1][1:]):raise ValueError('FONAR_REVENUE_HEADING732')
    totals=[(i,r) for i,r in nonempty if r and label(r[0])=='total revenues net']
    if len(totals)!=1:raise ValueError('FONAR_UNIQUE_PRIMARY_TOTAL732')
    ti,totalrow=totals[0];components=[r for i,r in nonempty[1:] if i<ti]
    base=['product sales net','service and repair fees net','service and repair fees related parties net'];tail=['management and other fees net','management and other fees related medical practices net']
    if years[0]>=2013:base+=['patient fee revenue, net of contractual allowances and discounts','provision for bad debts for patient fee']
    names=[label(r[0]) for r in components];normalized=[n.replace('related party medical practices','related medical practices') for n in names]
    if normalized!=base+tail:raise ValueError('FONAR_ALL_PRIMARY_COMPONENTS732')
    values=[amounts(r[1:],printed_zero=True) for r in components];totals=amounts(totalrow[1:])
    if len(totals)!=len(years) or any(len(v)!=len(years) for v in values) or components[0].count('$')!=len(years):raise ValueError('FONAR_PRIMARY_DOLLAR_COLUMNS732')
    if any(sum(v[i] for v in values)!=totals[i] for i in range(len(years))):raise ValueError('FONAR_NET_REVENUE_COMPONENT_EQUATION732')
    def line(name):
        hits=[r for r in rows[ti+1:] if r and label(r[0])==name]
        if len(hits)!=1:raise ValueError('FONAR_PRIMARY_OPERATING_HIERARCHY732')
        v=amounts(hits[0][1:])
        if len(v)!=len(years):raise ValueError('FONAR_PRIMARY_OPERATING_COLUMNS732')
        return v
    expenses=line('total costs and expenses');op=line('income from operations')
    if any(a-b!=c for a,b,c in zip(totals,expenses,op)):raise ValueError('FONAR_PRIMARY_OPERATING_EQUATION732')
    return {str(y)+'-06-30':v for y,v in zip(years,totals)}

def segments(rows,context):
    if not clean(context).endswith('summarized financial information concerning the company’s reportable segments is shown in the following table:'):raise ValueError('FONAR_REPORTABLE_SEGMENT_CONTEXT732')
    header=[clean(c) for c in rows[0] if c.strip()]
    if header!=['manufacturing and servicing of medical equipment','management of diagnostic imaging centers','totals']:raise ValueError('FONAR_SEGMENT_COLUMN_IDENTITY732')
    if len(rows) not in {17,25}:raise ValueError('FONAR_COMPLETE_SEGMENT_YEAR_BLOCKS732')
    result={};sequence=[]
    for offset in range(1,len(rows),8):
        block=rows[offset:offset+8];heading=[c.strip() for c in block[0] if c.strip()]
        if len(heading)!=1 or not re.fullmatch(r'Fiscal 20\d{2}:',heading[0]):raise ValueError('FONAR_SEGMENT_FISCAL_HEADING732')
        year=int(heading[0][7:11]);sequence.append(year)
        names=[label(r[0]).rstrip(' *') for r in block[1:]]
        if names!=['net revenues from external customers','intersegment net revenues','income from operations','depreciation and amortization','compensatory element of stock issuances','total identifiable assets','capital expenditures']:raise ValueError('FONAR_EXTERNAL_INTERSEGMENT_HIERARCHY732')
        external=amounts(block[1][1:],printed_zero=True);inter=amounts(block[2][1:],printed_zero=True)
        if block[1].count('$')!=3 or block[2].count('$')!=3 or len(external)!=3 or len(inter)!=3 or external[0]+external[1]!=external[2] or inter[0]+inter[1]!=inter[2] or inter[2]<=0:raise ValueError('FONAR_SEGMENT_SUM_UNIT_EQUATIONS732')
        result[str(year)+'-06-30']=dict(external=external[2],intersegment=inter[2])
    if sequence!=list(range(sequence[0],sequence[0]-len(sequence),-1)) or len(result)!=len(sequence):raise ValueError('FONAR_SEGMENT_ANNUAL_ORDER732')
    return result

def approve(scope,entry,root):
    r=entry['review'];metadata(r)
    if r['basis']!=BASIS or str(int(entry['cik']))!='355019':raise ValueError('FONAR_BASIS_COMPANY732')
    checked=reviewed_approval(scope,{**r,'basis':'UNCHANGED_TOTAL_CONFIRMED'},_reviewed_legacy_total=True)
    raw=verify(scope,r['originalRawEvidence'],root,company=478,cik='0000355019',raw_sha256=RAW_SHA256,metric='REVENUE',tags={'SalesRevenueNet','Revenues'})
    nodes=r['externalTotalEvidence']['nodes'];urls=[n['statement']['url'] for n in nodes];dates=[n['filingDate'] for n in nodes];expected=2 if r['end']=='2012-06-30' else 3
    if len(nodes)!=expected or len(set(urls))!=expected or dates!=sorted(set(dates)) or set(urls)!={x['source_url'] for x in scope} or dates[-1]!=r['filingDate']:raise ValueError('FONAR_ALL_SOURCE_REPORTS732')
    if {x['source_url'] for x in checked['proof']['originalSources']}!={urls[0]} or {x['source_url'] for x in checked['proof']['sources']}!={urls[-1]}:raise ValueError('FONAR_PRIMARY_ENDPOINTS732')
    vectors=[]
    for n in nodes:
        e=n['statement'];s=n['segments']
        if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']} or (s['url'],s['sha256'])!=(e['url'],e['sha256']):raise ValueError('FONAR_SAME_REPORT_DOCUMENT_BINDING732')
        rows,ctx,_=document(e,root);v=primary(rows,ctx);rows,ctx,_=document(s,root);seg=segments(rows,ctx)
        if set(v)!=set(seg) or r['end'] not in v or max(v)>n['filingDate'] or any(v[day]!=seg[day]['external'] for day in v):raise ValueError('FONAR_EVERY_PRIMARY_EXTERNAL_TOTAL732')
        vectors.append((v,seg))
    for (a,aseg),(b,bseg) in zip(vectors,vectors[1:]):
        common=set(a)&set(b)
        if not common or any(a[k]!=b[k] or aseg[k]!=bseg[k] for k in common):raise ValueError('FONAR_ALL_OVERLAPPING_PRIMARY_SEGMENT_COLUMNS732')
    legacy=modern=0
    for row in scope:
        i=urls.index(row['source_url']);v,seg=vectors[i]
        if row['form_type'] not in {'10-K','10-K/A'} or str(row['filing_date'])!=dates[i] or str(row['report_date'])!=max(v):raise ValueError('FONAR_EVERY_SOURCE_ANNUAL_PERIOD732')
        key='external' if row['source_tag']=='SalesRevenueNet' else 'intersegment';value=Decimal(str(row['value']))
        if value!=seg[r['end']][key] or key=='external' and value!=Decimal(r['value']):raise ValueError('FONAR_RAW_TAG_TOTAL_OR_INTERSEGMENT732')
        if key=='external':legacy+=1
        else:modern+=1
    if legacy!=expected or modern!=expected:raise ValueError('FONAR_COMPLETE_BOTH_TAG_OBSERVATIONS732')
    proof={**checked['proof'],'policy':POLICY,'review':r,'accountingCause':'EXPLICIT_EXTERNAL_CUSTOMER_TOTAL_VS_INTERSEGMENT_REVENUE','originalRawEvidence':raw,
           'allSourceReportsBound':True,'originalObservationsPreserved':True,'restatementClaimed':False,'servingTagContract':'EXPLICIT_COMPANY_PERIOD_LEGACY_STATEMENT_TOTAL699_V1','mappingScope':'COMPANY_METRIC_PERIOD_END_ONLY',
           'meaning':'Only two named Fonar periods. Full net-revenue components sum to the primary total; external-customer segments independently sum to that total. Other raw revenue tags match intersegment sales, which are not added to consolidated external revenue. No global legacy tag fallback or other metric approval.'}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
