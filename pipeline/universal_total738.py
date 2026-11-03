"""Two named Universal total-revenue scopes, never a global legacy fallback.

Customer-contract revenue is only a subtotal. Both original tags and original
filing dates remain intact; later primary totals and their note bridge must
independently agree before selecting the original full-statement total.
"""
import gzip
import hashlib
import re
from decimal import Decimal
from html.parser import HTMLParser
from pipeline.legacy_statement_total699 import clean, document
from pipeline.reviewed_financial_mapping672 import reviewed_approval, digest
from pipeline.original_sec_scope725 import verify

BASIS = 'REVIEWED_UNIVERSAL_CONTRACT_SUBTOTAL_TOTAL738'
POLICY = 'sec-universal-contract-subtotal-total738-v1'
RAW_SHA256 = '6c16c913e55bbba701cb1b3de6cb285dda453dd30c13a216b4737797804206db'
MODERN = 'RevenueFromContractWithCustomerExcludingAssessedTax'
NOTE_UNIT = 'universal corporation notes to consolidated financial statements (all dollar amounts are in thousands, except per share amounts or as otherwise noted.)'


def metadata(r):
    if (r['companyId'], r['metricId'], r['metric'], r['chosenTag']) != (366, 22, 'REVENUE', 'SalesRevenueNet') or (r['start'], r['end']) not in {('2016-04-01','2017-03-31'), ('2017-04-01','2018-03-31')}:
        raise ValueError('UNIVERSAL_ONLY_TWO_NAMED_TOTAL_SCOPES738')


def amounts(cells):
    parts = [c.strip() for c in cells if c.strip() and c.strip() != '$']
    values = []
    i = 0
    while i < len(parts):
        s = parts[i]
        if s.startswith('(') and not s.endswith(')') and i+1 < len(parts) and parts[i+1] == ')':
            s += ')'; i += 1
        if s == '\u2014':
            values.append(Decimal(0))
        elif re.fullmatch(r'\(?\d{1,3}(?:,\d{3})*\)?', s) and s.startswith('(') == s.endswith(')'):
            values.append(Decimal(s.strip('()').replace(',', '')) * (-1 if s.startswith('(') else 1) * 1000)
        else:
            raise ValueError('UNIVERSAL_LITERAL_THOUSAND_DOLLAR_VALUES738')
        i += 1
    if len(values) != 3:
        raise ValueError('UNIVERSAL_THREE_NUMERIC_COLUMNS738')
    return values


def headers(rows, *, primary):
    rows = [r for r in rows if any(c.strip() for c in r)]
    if not rows or clean(' '.join(rows[0])) != 'fiscal year ended march 31,':
        raise ValueError('UNIVERSAL_ACTUAL_MARCH31_ANNUAL738')
    h = [c.strip() for c in rows[1] if c.strip()]
    if primary:
        if clean(h[0]) != '(in thousands of dollars, except share and per share data)':
            raise ValueError('UNIVERSAL_PRIMARY_UNIT738')
        h = h[1:]
    if len(h) != 3 or any(not re.fullmatch(r'20\d{2}', c) for c in h):
        raise ValueError('UNIVERSAL_EXACT_YEAR_COLUMNS738')
    years = list(map(int, h))
    allowed = {2017,2018,2019,2020} if primary else {2019,2020}
    if years[0] not in allowed or years != list(range(years[0], years[0]-3, -1)):
        raise ValueError('UNIVERSAL_ANNUAL_SEQUENCE738')
    return rows, [str(y)+'-03-31' for y in years]


def primary(rows, context):
    if not clean(context).endswith('universal corporation consolidated statements of income'):
        raise ValueError('UNIVERSAL_FULL_PRIMARY_IDENTITY738')
    rows, dates = headers(rows, primary=True)
    def line(label, optional=False):
        hits = [r for r in rows[2:] if r and clean(r[0]) == label]
        if optional and not hits: return [Decimal(0)] * 3
        if len(hits) != 1: raise ValueError('UNIVERSAL_COMPLETE_PRIMARY_HIERARCHY738')
        return amounts(hits[0][1:])
    revenue = line('sales and other operating revenues')
    if rows[2][0].strip().lower() != 'sales and other operating revenues' or rows[2].count('$') != 3:
        raise ValueError('UNIVERSAL_PRIMARY_TOTAL_DOLLAR_ROW738')
    cost, sga, other, restructuring, op = [line(n, n == 'other income') for n in ['cost of goods sold','selling, general and administrative expenses','other income','restructuring and impairment costs','operating income']]
    if any(a-b-c-d-e != f for a,b,c,d,e,f in zip(revenue,cost,sga,other,restructuring,op)):
        raise ValueError('UNIVERSAL_FULL_PRIMARY_OPERATING_EQUATIONS738')
    return dict(zip(dates, revenue))


def contracts(rows, context):
    if not clean(context).endswith('disaggregation of revenue from contracts with customers the following table disaggregates the company\u2019s revenue by significant revenue-generating category:'):
        raise ValueError('UNIVERSAL_CONTRACT_NOTE_IDENTITY738')
    rows, dates = headers(rows, primary=False)
    labels = [clean(r[0]) for r in rows[2:]]
    if len(rows) != 8 or labels[0] != 'tobacco sales' or labels[1] not in {'processing revenue','tobacco processing revenue'} or labels[2:] != ['other sales and revenue from contracts with customers','total revenue from contracts with customers','other operating sales and revenues','consolidated sales and other operating revenues']:
        raise ValueError('UNIVERSAL_CONTRACT_SUBTOTAL_PARENT_HIERARCHY738')
    v = [amounts(r[1:]) for r in rows[2:]]
    if rows[2].count('$') != 3 or rows[-1].count('$') != 3 or any(a+b+c != d or d+e != f or min(a,b,c,d,e,f)<0 for a,b,c,d,e,f in zip(*v)):
        raise ValueError('UNIVERSAL_ALL_CONTRACT_AND_TOTAL_SUMS738')
    return {day: dict(contract=c,other=o,total=t) for day,c,o,t in zip(dates,v[3],v[4],v[5])}


class Visible(HTMLParser):
    def __init__(self): super().__init__(convert_charrefs=True); self.hidden=0; self.text=[]
    def handle_starttag(self,tag,attrs):
        if tag in {'script','style'}: self.hidden += 1
    def handle_endtag(self,tag):
        if tag in {'script','style'}: self.hidden=max(0,self.hidden-1)
    def handle_data(self,s):
        if not self.hidden: self.text.append(s)


def notes_unit(binding, root):
    with gzip.open(root/'documents'/(binding['sha256']+'.html.gz'),'rb') as f: body=f.read(32*1024**2+1)
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=binding['sha256']: raise ValueError('UNIVERSAL_NOTES_SOURCE_HASH738')
    p=Visible(); p.feed(body.decode('utf8','strict')); text=clean(' '.join(p.text))
    marker='disaggregation of revenue from contracts with customers the following table disaggregates the company\u2019s revenue by significant revenue-generating category:'
    if NOTE_UNIT not in text or marker not in text or text.index(NOTE_UNIT)>=text.index(marker):
        raise ValueError('UNIVERSAL_EXPLICIT_GLOBAL_NOTE_THOUSANDS_REQUIRED738')
    # Unit belongs to this named report's notes; numeric coincidence is not evidence.
    return digest(NOTE_UNIT)


def approve(scope, entry, root):
    r=entry['review']; metadata(r)
    if r['basis']!=BASIS or str(int(entry['cik']))!='102037': raise ValueError('UNIVERSAL_BASIS_CIK738')
    checked=reviewed_approval(scope,{**r,'basis':'UNCHANGED_TOTAL_CONFIRMED'},_reviewed_legacy_total=True)
    raw=verify(scope,r['originalRawEvidence'],root,company=366,cik='0000102037',raw_sha256=RAW_SHA256,metric='REVENUE',tags={'SalesRevenueNet',MODERN})
    nodes=r['contractTotalEvidence']['nodes']; urls=[n['statement']['url'] for n in nodes]; dates=[n['filingDate'] for n in nodes]
    if len(nodes)!=3 or len(set(urls))!=3 or dates!=sorted(set(dates)) or set(urls)!={x['source_url'] for x in scope}:
        raise ValueError('UNIVERSAL_ALL_THREE_REPORTS738')
    primary_vectors=[]; note_vectors=[]; units=[]
    for n in nodes:
        e=n['statement']
        if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}: raise ValueError('UNIVERSAL_DOCUMENT_BINDING738')
        rows,ctx,_=document(e,root); v=primary(rows,ctx)
        if r['end'] not in v or max(v)>n['filingDate'] or v[r['end']]!=Decimal(r['value']): raise ValueError('UNIVERSAL_ALL_PRIMARY_TOTALS_UNCHANGED738')
        primary_vectors.append(v); note=n.get('contracts'); seg=None
        if note:
            if (note['url'],note['sha256'])!=(e['url'],e['sha256']): raise ValueError('UNIVERSAL_SAME_REPORT_NOTE738')
            units.append(notes_unit(note,root)); rows,ctx,_=document(note,root); seg=contracts(rows,ctx)
            if set(seg)!=set(v) or any(v[k]!=seg[k]['total'] for k in v): raise ValueError('UNIVERSAL_EVERY_NOTE_PRIMARY_COLUMN738')
        note_vectors.append(seg)
    for a,b in zip(primary_vectors,primary_vectors[1:]):
        common=set(a)&set(b)
        if len(common)!=2 or any(a[k]!=b[k] for k in common): raise ValueError('UNIVERSAL_ALL_PRIMARY_COMPARATIVES738')
    legacy=modern=0
    for row in scope:
        i=urls.index(row['source_url']); tag=row['source_tag']; v=primary_vectors[i]
        if row['form_type'] not in {'10-K','10-K/A'} or str(row['filing_date'])!=dates[i] or str(row['report_date'])!=max(v): raise ValueError('UNIVERSAL_EVERY_RAW_REPORT_PERIOD738')
        if tag=='SalesRevenueNet':
            if Decimal(str(row['value']))!=v[r['end']]: raise ValueError('UNIVERSAL_LEGACY_FULL_TOTAL738')
            legacy+=1
        elif tag==MODERN:
            seg=note_vectors[i]
            if not seg or Decimal(str(row['value']))!=seg[r['end']]['contract'] or seg[r['end']]['other']<=0: raise ValueError('UNIVERSAL_MODERN_CONTRACT_SUBTOTAL738')
            modern+=1
        else: raise ValueError('UNIVERSAL_UNREGISTERED_TAG738')
    if (legacy,modern)!=((2,1) if r['end']=='2017-03-31' else (1,2)): raise ValueError('UNIVERSAL_COMPLETE_BOTH_TAG_OBSERVATIONS738')
    available=[v for v in note_vectors if v]
    for a,b in zip(available,available[1:]):
        common=set(a)&set(b)
        if len(common)!=2 or any(a[k]!=b[k] for k in common): raise ValueError('UNIVERSAL_ALL_NOTE_COMPARATIVES738')
    proof={**checked['proof'],'policy':POLICY,'review':r,'originalRawEvidence':raw,'accountingCause':'CONTRACT_REVENUE_SUBTOTAL_EXCLUDES_OTHER_OPERATING_REVENUE','allSourceReportsBound':True,'originalObservationsPreserved':True,'restatementClaimed':False,'explicitNoteUnitHashes':units,'servingTagContract':'EXPLICIT_COMPANY_PERIOD_LEGACY_STATEMENT_TOTAL699_V1','mappingScope':'COMPANY_METRIC_PERIOD_END_ONLY','meaning':'Only Universal FY2017/FY2018 consolidated sales and other operating revenue. Contract revenue plus other operating revenue equals the unchanged primary total in every note column. Original selected-tag filing dates are retained, not relabelled as later filings. Other metrics and periods need separate review.'}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
