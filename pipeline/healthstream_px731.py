"""HealthStream 2016/2017 continuing revenue after explicit PX disposal.

All three source reports and both comparative columns per link are mandatory.
ASC 606's modified-retrospective adoption is bound separately: prior periods
were not restated for that standard. No organic-growth inference is made.
"""
import gzip,hashlib,re
from decimal import Decimal
from pipeline.legacy_statement_total699 import document,clean
from pipeline.reviewed_financial_mapping672 import reviewed_approval,digest
from pipeline.original_sec_scope725 import verify

BASIS='REVIEWED_HEALTHSTREAM_PX_DISCONTINUED_REVENUE731'
POLICY='sec-healthstream-px-discontinued731-v1'
RAW_SHA256='044ab0f6e8d1708e92259597602644ff618726b78cea2ec95f71913048f52148'
ADOPTION_SHA256='cbddaa710152f174d97a790bd91500f98349daa74df76e06b01d875d1f7b279c'
PERIODS={('2016-01-01','2016-12-31'),('2017-01-01','2017-12-31')}
ADOPTION=("on january 1, 2018, the company adopted asc 606 using the modified retrospective approach applied to contracts not completed as of january 1, 2018.",
          "as such, results for reporting periods beginning after january 1, 2018 are presented under asc 606, while prior period amounts continue to be reported in accordance with asc 605.")
PX_CAUSE='the company has classified the results of its px business segment as discontinued operations in its consolidated statements of income and cash flows for all periods presented.'

def metadata(r):
    if (r['companyId'],r['metricId'],r['metric'],r['chosenTag'])!=(1615,22,'REVENUE','RevenueFromContractWithCustomerExcludingAssessedTax') or (r['start'],r['end']) not in PERIODS:
        raise ValueError('HEALTHSTREAM_NAMED_PX_SCOPE731')

def vector(rows,context,*,px=False):
    ctx=clean(context)
    if px:
        if PX_CAUSE not in ctx or not ctx.endswith('the following table presents the financial results of the px business (in thousands):'):
            raise ValueError('HEALTHSTREAM_EXPLICIT_PX_TABLE_CAUSE731')
    elif not ctx.endswith('healthstream, inc. consolidated statements of income (in thousands, except per share data)'):
        raise ValueError('HEALTHSTREAM_FULL_PRIMARY_UNIT_TITLE731')
    years=[];yr=None
    for i,row in enumerate(rows[:3]):
        values=[c.strip() for c in row if c.strip()]
        if values and all(re.fullmatch(r'20\d{2}',c) for c in values):
            if years:raise ValueError('HEALTHSTREAM_DUPLICATE_YEAR_ROW731')
            years=list(map(int,values));yr=i
    if len(years) not in ([2,3] if px else [3]) or years!=list(range(years[0],years[0]-len(years),-1)):
        raise ValueError('HEALTHSTREAM_EXACT_ANNUAL_COLUMNS731')
    heading=clean(' '.join(' '.join(row) for row in rows[:yr]))
    if heading not in {'year ended december 31,','for the year ended december 31,'}:raise ValueError('HEALTHSTREAM_EXPLICIT_CALENDAR_ANNUAL731')
    found=[r for r in rows[yr+1:] if r and clean(r[0])=='revenues, net']
    if len(found)!=1 or found[0].count('$')!=len(years):raise ValueError('HEALTHSTREAM_NET_REVENUE_LINE731')
    cells=[c.strip() for c in found[0][1:] if c.strip() and c.strip()!='$']
    if len(cells)!=len(years) or any(not re.fullmatch(r'\d{1,3}(?:,\d{3})*',c) for c in cells):raise ValueError('HEALTHSTREAM_LITERAL_THOUSAND_REVENUE731')
    if not any(r and clean(r[0])=='operating costs and expenses:' for r in rows) or not any(r and 'cost of revenues (excluding depreciation and amortization)'==clean(r[0]) for r in rows):
        raise ValueError('HEALTHSTREAM_COMPLETE_REVENUE_HIERARCHY731')
    return {str(y)+'-12-31':Decimal(c.replace(',',''))*1000 for y,c in zip(years,cells)}

def adoption(binding,root):
    if binding['sha256']!=ADOPTION_SHA256:raise ValueError('HEALTHSTREAM_ADOPTION_SOURCE_PIN731')
    path=root/'documents'/(binding['sha256']+'.html.gz')
    if path.is_symlink():raise ValueError('HEALTHSTREAM_ADOPTION_PATH731')
    with gzip.open(path,'rb') as f:body=f.read(32*1024**2+1)
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=binding['sha256']:raise ValueError('HEALTHSTREAM_ADOPTION_BYTES731')
    from pipeline.reconciliation_review679 import Tables
    class Visible(Tables):
        def __init__(self):super().__init__();self.visible=[]
        def handle_data(self,data):
            if not self.hidden:self.visible.append(data)
            super().handle_data(data)
    p=Visible();p.feed(body.decode('utf8','replace'));text=clean(' '.join(p.visible));positions=[]
    for clause in ADOPTION:
        matches=[m.start() for m in re.finditer(re.escape(clause),text)]
        if len(matches)!=1:raise ValueError('HEALTHSTREAM_PRIOR_PERIOD_STANDARD_NOT_RESTATED731')
        positions.append(matches[0])
    if not 0<positions[1]-positions[0]<1000:raise ValueError('HEALTHSTREAM_BOUND_STANDARD_SECTION731')
    return dict(url=binding['url'],sha256=binding['sha256'],priorPeriodsNotRestatedForASC606=True,clauses=list(ADOPTION))

def approve(scope,entry,root):
    r=entry['review'];metadata(r)
    if r['basis']!=BASIS or str(int(entry['cik']))!='1095565':raise ValueError('HEALTHSTREAM_BASIS_COMPANY731')
    checked=reviewed_approval(scope,{**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD','revenueStandardAdjustment':str(Decimal(r['value'])-Decimal(r['originalValue']))})
    raw=verify(scope,r['originalRawEvidence'],root,company=1615,cik='0001095565',raw_sha256=RAW_SHA256,metric='REVENUE',tags={'SalesRevenueNet','Revenues','RevenueFromContractWithCustomerExcludingAssessedTax'})
    ev=r['pxPrimaryEvidence'];nodes=ev['nodes'];urls=[n['statement']['url'] for n in nodes];dates=[n['filingDate'] for n in nodes]
    if len(nodes)!=3 or len(set(urls))!=3 or dates!=sorted(set(dates)) or set(urls)!={x['source_url'] for x in scope} or dates[-1]!=r['filingDate']:raise ValueError('HEALTHSTREAM_ALL_SOURCE_REPORTS731')
    if {x['source_url'] for x in checked['proof']['originalSources']}!={urls[0]} or {x['source_url'] for x in checked['proof']['sources']}!={urls[-1]}:raise ValueError('HEALTHSTREAM_PRIMARY_ENDPOINT_REPORTS731')
    if (ev['adoption']['url'],ev['adoption']['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:raise ValueError('HEALTHSTREAM_ADOPTION_DOCUMENT_BINDING731')
    standard=adoption(ev['adoption'],root);vectors=[]
    for n in nodes:
        e=n['statement']
        if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:raise ValueError('HEALTHSTREAM_PRIMARY_DOCUMENT_BINDING731')
        rows,ctx,_=document(e,root);v=vector(rows,ctx);px=None
        if r['end'] not in v or max(v)>n['filingDate']:raise ValueError('HEALTHSTREAM_PRIMARY_TARGET_DATE731')
        if n.get('px'):
            if (n['px']['url'],n['px']['sha256'])!=(e['url'],e['sha256']):raise ValueError('HEALTHSTREAM_SAME_REPORT_PX_NOTE731')
            rows,ctx,_=document(n['px'],root);px=vector(rows,ctx,px=True)
            if r['end'] not in px or not set(px)<=set(v):raise ValueError('HEALTHSTREAM_PX_ACTUAL_PERIODS731')
        vectors.append((v,px))
    transitions=0;overlaps=[]
    for (a,apx),(b,bpx) in zip(vectors,vectors[1:]):
        common=sorted(set(a)&set(b))
        if len(common)!=2:raise ValueError('HEALTHSTREAM_BOTH_COMPARATIVE_COLUMNS731')
        if apx is None and bpx is not None:
            transitions+=1
            if any(day not in bpx or a[day]-bpx[day]!=b[day] for day in common):raise ValueError('HEALTHSTREAM_EXACT_PX_REPRESENTATION731')
        else:
            if apx is not None and bpx is None:raise ValueError('HEALTHSTREAM_PX_REVERSAL731')
            if any(a[day]!=b[day] for day in common):raise ValueError('HEALTHSTREAM_UNCHANGED_OTHER_COMPARATIVES731')
            if apx is not None and any(day not in apx or day not in bpx or apx[day]!=bpx[day] for day in common):raise ValueError('HEALTHSTREAM_PX_COMPARATIVE_VALUE_CHANGED731')
        overlaps.append(common)
    if transitions!=1 or vectors[0][1] is not None or vectors[-1][1] is None:raise ValueError('HEALTHSTREAM_ONE_EXPLICIT_PX_TRANSITION731')
    for row in scope:
        i=urls.index(row['source_url']);v=vectors[i][0]
        if row['form_type'] not in {'10-K','10-K/A'} or str(row['filing_date'])!=dates[i] or str(row['report_date'])!=max(v) or Decimal(str(row['value']))!=v[r['end']]:raise ValueError('HEALTHSTREAM_EVERY_RAW_PRIMARY_PERIOD_VALUE731')
    proof={**checked['proof'],'policy':POLICY,'review':r,'accountingCause':'EXPLICIT_PX_DISCONTINUED_OPERATIONS_REPRESENTATION','originalRawEvidence':raw,'revenueStandard':standard,
           'overlappingActualPeriodEnds':overlaps,'allSourceReportsBound':True,'originalObservationsPreserved':True,
           'discontinuedRevenueUSD':str(vectors[0][0][r['end']]-vectors[-1][0][r['end']]),
           'meaning':'Only named 2016/2017 revenue: original total less explicitly discontinued PX net revenue equals continuing net revenue across both comparative columns. ASC606 did not restate these prior periods. Not organic growth or a stand-alone PX valuation.'}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
