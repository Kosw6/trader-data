"""CENX full primary revenue totals with exact related-party components.

Starred comparative years require the actual pension-presentation footnote in
the operations section. No assumption is made about any other financial metric.
"""
import gzip,hashlib,re
from datetime import date
from decimal import Decimal,ROUND_HALF_UP
from html.parser import HTMLParser
from pipeline.legacy_statement_total699 import document,clean
from pipeline.reconciliation_review679 import numeric
from pipeline.reviewed_financial_mapping672 import reviewed_approval,digest
from pipeline.financial_mapping667 import invalid

BASIS='REVIEWED_RELATED_PARTY_PRIMARY_PRECISION714'
POLICY='sec-reviewed-related-party-primary-precision714-v1'
CONTRACT='CENX_RELATED_PARTY_FULL_PRIMARY714_V1'
FOOTNOTE='* as adjusted due to the adoption of asu 2017-07 compensation - retirement benefits (topic 715): improving the presentation of net periodic pension cost and net periodic postretirement benefit cost'


class VisibleText(HTMLParser):
    def __init__(self):super().__init__();self.parts=[];self.hidden=0
    def handle_starttag(self,tag,attrs):
        if tag in {'style','script'}:self.hidden+=1
    def handle_endtag(self,tag):
        if tag in {'style','script'}:self.hidden=max(0,self.hidden-1)
    def handle_data(self,data):
        if not self.hidden:self.parts.append(data)


def pension_footnote(e,root):
    with gzip.open(root/'documents'/(e['sha256']+'.html.gz'),'rb') as f:body=f.read(32*1024**2+1)
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=e['sha256']:raise ValueError('RELATED_PRIMARY_SOURCE_CHANGED')
    p=VisibleText();p.feed(body.decode('utf8','replace'));text=clean(' '.join(p.parts));sections=[]
    for m in re.finditer(r'consolidated statements of operations \(in millions, except per share amounts\) year ended december 31, 2018 2017\* 2016\*',text):
        stop=text.find('consolidated statements of comprehensive income',m.end())
        if stop<0 or stop-m.start()>12000:continue
        section=text[m.start():stop]
        if section.count(FOOTNOTE)!=1 or section.count('* as adjusted')!=1:continue
        sections.append(section)
    if len(sections)!=1:raise ValueError('RELATED_PRIMARY_EXACT_PENSION_FOOTNOTE')
    return dict(footnoteHash=digest(FOOTNOTE),sectionHash=digest(sections[0]),meaning='DISCLOSED_PENSION_COST_PRESENTATION_ONLY_NO_OTHER_METRIC_APPROVAL')


def vector(entry,root,node):
    e=node['statement']
    if str(int(entry['cik']))!='949157' or e.get('totalContract')!=CONTRACT or e.get('periodMode')!='EXACT_YEAR_STAR714':raise ValueError('RELATED_PRIMARY_COMPANY_CONTRACT')
    if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:raise ValueError('RELATED_PRIMARY_DOCUMENT_IDENTITY')
    rows,context,_=document(e,root)
    if (e.get('periodRow'),e.get('valueRow'))!=(6,10) or len(rows)<20:raise ValueError('RELATED_PRIMARY_ROW_BINDING')
    if [clean(' '.join(rows[i])) for i in (2,3,5)]!=['century aluminum company','consolidated statements of operations','year ended december 31,']:raise ValueError('RELATED_PRIMARY_FULL_STATEMENT_HEADING')
    units=[clean(c) for c in rows[4] if clean(c)]
    if len(units)!=1 or units[0] not in {'(in thousands, except per share amounts)','(in millions, except per share amounts)'}:raise ValueError('RELATED_PRIMARY_EXACT_UNIT')
    scale=Decimal(1000 if 'thousands' in units[0] else 1000000);places=node['decimalPlaces']
    if Decimal(str(e['unitScale']))!=scale or type(places) is not int or places!=(0 if scale==1000 else 1):raise ValueError('RELATED_PRIMARY_PRECISION')
    labels=[clean(c) for c in rows[6] if clean(c)]
    if len(labels)!=3 or any(not re.fullmatch(r'(?:19|20)\d{2}\*?',v) for v in labels):raise ValueError('RELATED_PRIMARY_YEAR_COLUMNS')
    starred=any('*' in v for v in labels)
    footnote=pension_footnote(e,root) if starred else None
    if starred and (labels!=['2018','2017*','2016*'] or node.get('footnoteHash')!=footnote['footnoteHash'] or node.get('footnoteSectionHash')!=footnote['sectionHash']):raise ValueError('RELATED_PRIMARY_STAR_SCOPE_BINDING')
    if not starred and (node.get('footnoteHash') or node.get('footnoteSectionHash')):raise ValueError('RELATED_PRIMARY_UNBOUND_FOOTNOTE')
    years=[int(v.rstrip('*')) for v in labels]
    if any(a-b!=1 for a,b in zip(years,years[1:])):raise ValueError('RELATED_PRIMARY_ANNUAL_ORDER')
    if [clean(rows[i][0]) for i in (7,8,9,10)]!=['net sales:','related parties','other customers','total net sales'] or clean(e['rowLabel'])!='total net sales':raise ValueError('RELATED_PRIMARY_COMPONENT_HIERARCHY')
    if any(numeric(c) is not None for c in rows[7][1:]):raise ValueError('RELATED_PRIMARY_COMPONENT_HEADING')
    values=[]
    for row in rows[8:11]:
        cells=[c for c in row[1:] if numeric(c) is not None]
        if len(cells)!=3 or any('%' in c for c in row) or any(not re.fullmatch(r'\d+(?:,\d{3})*'+(r'\.\d' if places else ''),c.strip()) for c in cells):raise ValueError('RELATED_PRIMARY_AMOUNT_COLUMNS')
        values.append([numeric(c)*scale for c in cells])
    if any(a+b!=c for a,b,c in zip(*values)):raise ValueError('RELATED_PRIMARY_ALL_COMPONENT_EQUATIONS')
    keys=[f'{y}-12-31' for y in years]
    if any(date.fromisoformat(k)>date.fromisoformat(node['filingDate']) for k in keys):raise ValueError('RELATED_PRIMARY_FUTURE_COLUMN')
    return dict(zip(keys,values[2])),scale/(Decimal(10)**places)


def approve(scope,entry,root):
    r=entry['review'];nodes=r['primaryPrecisionEvidence']['nodes']
    if r['basis']!=BASIS or r['companyId']!=1258 or r['metric']!='REVENUE' or len(nodes)!=3:raise ValueError('RELATED_PRIMARY_REVIEW_SCOPE')
    checked=reviewed_approval(scope,{**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD','revenueStandardAdjustment':str(Decimal(r['value'])-Decimal(r['originalValue']))})
    urls=[n['statement']['url'] for n in nodes];dates=[n['filingDate'] for n in nodes]
    if len(set(urls))!=3 or dates!=sorted(set(dates)) or dates[-1]!=r['filingDate'] or {x['source_url'] for x in scope}!=set(urls):raise ValueError('RELATED_PRIMARY_ALL_REPORTS_IN_ORDER')
    if {x['source_url'] for x in checked['proof']['originalSources']}!={urls[0]} or {x['source_url'] for x in checked['proof']['sources']}!={urls[-1]}:raise ValueError('RELATED_PRIMARY_ENDPOINT_REPORTS')
    vectors=[vector(entry,root,n) for n in nodes];target=r['end']
    if any(target not in v for v,q in vectors) or vectors[0][0][target]!=Decimal(r['originalValue']) or vectors[-1][0][target]!=Decimal(r['value']):raise ValueError('RELATED_PRIMARY_ENDPOINT_VALUES')
    for row in scope:
        shadow=dict(row,report_date=row['period_end'])
        if row['source_tag']=='SalesRevenueNet':shadow['source_tag']='Revenues'
        if invalid(shadow,annual_only=True):raise ValueError('RELATED_PRIMARY_SOURCE_ANNUAL')
        i=urls.index(row['source_url'])
        if str(row['filing_date'])!=dates[i] or Decimal(str(row['value']))!=vectors[i][0][target]:raise ValueError('RELATED_PRIMARY_RAW_REPORT_VALUE')
    if vectors[-1][1]<=vectors[0][1]:raise ValueError('RELATED_PRIMARY_NO_PRECISION_REDUCTION')
    def rounded(v,q):return (v/q).quantize(Decimal(1),rounding=ROUND_HALF_UP)*q
    overlaps=[]
    for (a,aq),(b,bq) in zip(vectors,vectors[1:]):
        common=sorted(set(a)&set(b))
        if len(common)<2 or bq<aq or any(rounded(a[k],bq)!=b[k] for k in common):raise ValueError('RELATED_PRIMARY_COMPARATIVE_CONFLICT')
        overlaps.append(common)
    proof={**checked['proof'],'policy':POLICY,'review':r,'quantumsUSD':[str(q) for v,q in vectors],'overlappingPeriodEnds':overlaps,'allSourceReportsBound':True,'originalObservationsPreserved':True,'accountingCause':'PUBLISHED_PRIMARY_REVENUE_PRECISION_COMPATIBILITY_WITH_PENSION_PRESENTATION_FOOTNOTE','meaning':'Revenue components and comparative totals match at published precision. The pension-presentation footnote is source-bound; no approval or inference for income or other metrics.'}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
