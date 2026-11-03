"""Explicit plural unit declaration binds two consecutive ASC606 schedules.

Revenue component equations are checked for both disclosed years; unrelated
expense/income rows do not inherit a revenue approval.
"""
import gzip,hashlib,re
from datetime import datetime
from decimal import Decimal
from pipeline.reconciliation_review679 import Tables,numeric
from pipeline.legacy_statement_total699 import clean,DATE,document
from pipeline.related_precision714 import VisibleText
from pipeline.reviewed_financial_mapping672 import digest,reviewed_approval
from pipeline.financial_mapping667 import invalid
from pipeline.original_sec_scope725 import verify as verify_original

BASIS='REVIEWED_EXTR_SHARED_UNIT_FULL_RETROSPECTIVE725'
POLICY='sec-extr-shared-unit-all-primary725-v1'
SOURCE_SHA256='c72134b231b8e31b92b4b88c919614fdb331a8326d0f47eb80cad9946305fe13'
TABLE_HASHES=['2c8eb9754e9f831835d759555a0c8087687d19c05f293c6d3cb5a9354f9ebdaa','db927f87f443d5f9f73ad27c85be0acd46bca5c696acbb8c7a78d82c71b1b116']
URL='https://www.sec.gov/Archives/edgar/data/1078271/000156459018022453/extr-10k_20180630.htm'
RAW_SHA256='1a3b6df96b7657d202af1f2c8a73495e72078e9ddfaa3f31f18899c130ee5b1c'
DECLARATION='the following tables present the effect of the adoption of asu 2014-09 on the company’s consolidated statements of operations for the years ended june 30, 2017 and 2016 (in thousands, except per share amounts):'
CAUSE='the company adopted topic 606 on july 1, 2017, using the full retrospective method.'


def components(rows,indices):
    labels=[clean(rows[i][0]) for i in indices]
    if labels!=['product','service','total net revenues']:raise ValueError('EXTR_REVENUE_COMPONENT_LABELS725')
    vectors=[]
    for i in indices:
        values=[v for cell in rows[i][1:] if (v:=(Decimal(0) if cell.strip() in {'—','–','-'} else numeric(cell))) is not None]
        if len(values)!=3 or any('%' in c for c in rows[i]):raise ValueError('EXTR_REVENUE_COMPONENT_COLUMNS725')
        vectors.append(values)
    if any(a+b!=total for a,b,total in zip(*vectors)):
        raise ValueError('EXTR_REVENUE_COMPONENT_EQUATIONS725')
    return [[v*1000 for v in vector] for vector in vectors]


def adjustment_source(body):
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=SOURCE_SHA256:raise ValueError('EXTR_ADOPTION_SOURCE_CHANGED725')
    p=Tables();p.feed(body.decode('utf8','replace'));v=VisibleText();v.feed(body.decode('utf8','replace'));text=clean(' '.join(v.parts))
    if CAUSE not in text:raise ValueError('EXTR_FULL_RETROSPECTIVE_CAUSE725')
    found=[(i,rows,ctx) for i,(rows,ctx) in enumerate(zip(p.tables,p.contexts)) if digest(rows) in TABLE_HASHES]
    if len(found)!=2 or [digest(rows) for i,rows,ctx in found]!=TABLE_HASHES or found[1][0]!=found[0][0]+1:raise ValueError('EXTR_CONSECUTIVE_ADJUSTMENT_TABLES725')
    if not clean(found[0][2]).endswith(DECLARATION):raise ValueError('EXTR_PLURAL_SHARED_UNIT_DECLARATION725')
    later_units=set(re.findall(r'\bin (thousands|millions)\b',clean(found[1][2])))
    if later_units-{'thousands'}:raise ValueError('EXTR_SECOND_TABLE_UNIT_CONFLICT725')
    result={}
    for (i,rows,ctx),year in zip(found,[2017,2016]):
        if clean(' '.join(rows[0]))!=f'year ended june 30, {year}' or [clean(c) for c in rows[1] if clean(c)]!=['as reported','adjustment','as adjusted'] or clean(rows[2][0])!='net revenues':raise ValueError('EXTR_ADJUSTMENT_ANNUAL_ROLES725')
        product,service,total=components(rows,[3,4,5])
        if any(old+change!=new for old,change,new in [product,service,total]):raise ValueError('EXTR_ADJUSTMENT_ALL_REVENUE_EQUATIONS725')
        result[f'{year}-06-30']=dict(previous=str(total[0]),adjustment=str(total[1]),current=str(total[2]),documentSha256=SOURCE_SHA256,tableHash=digest(rows),unitScale='1000',unitSource='EXPLICIT_PLURAL_DECLARATION_FOR_TWO_CONSECUTIVE_TABLES',declarationHash=digest(DECLARATION),causeHash=digest(CAUSE))
    return result


def primary_vector(rows,context):
    if not clean(context).endswith('extreme networks, inc. consolidated statements of operations (in thousands, except per share amounts)'):
        raise ValueError('EXTR_PRIMARY_TITLE_UNIT725')
    if clean(' '.join(rows[0]))!='year ended':raise ValueError('EXTR_PRIMARY_ANNUAL_HEADER725')
    matches=list(DATE.finditer(' '.join(rows[1])))
    if len(matches)!=3:raise ValueError('EXTR_PRIMARY_ACTUAL_DATES725')
    ends=[datetime.strptime(' '.join(m.groups()),'%B %d %Y').date().isoformat() for m in matches]
    if any(not e.endswith('-06-30') for e in ends) or any(int(a[:4])-int(b[:4])!=1 for a,b in zip(ends,ends[1:])):raise ValueError('EXTR_PRIMARY_ANNUAL_DATE_ORDER725')
    offset=3 if clean(rows[2][0])=='net revenues:' else 4 if clean(rows[3][0])=='net revenues:' else None
    if offset is None:raise ValueError('EXTR_PRIMARY_COMPONENT_HIERARCHY725')
    if offset==4:
        labels=[clean(c) for c in rows[2] if clean(c)]
        if ends!=['2018-06-30','2017-06-30','2016-06-30'] or labels!=['(as adjusted)','(as adjusted)']:raise ValueError('EXTR_PRIMARY_RECAST_COLUMNS725')
    product,service,total=components(rows,[offset,offset+1,offset+2])
    if not any(clean(r[0])=='cost of revenues:' for r in rows[offset+3:] if r):raise ValueError('EXTR_FULL_PRIMARY_REQUIRED725')
    return dict(zip(ends,total))


def approve(scope,entry,root):
    r=entry['review']
    if r['basis']!=BASIS or (r['companyId'],r['metricId'],r['metric'],r['start'],r['end'])!=(1565,22,'REVENUE','2015-07-01','2016-06-30') or str(int(entry['cik']))!='1078271':raise ValueError('EXTR_EXACT_REVIEW_SCOPE725')
    original=verify_original(scope,r['originalRawEvidence'],root,company=1565,cik='0001078271',raw_sha256=RAW_SHA256,metric='REVENUE',tags={'SalesRevenueNet','RevenueFromContractWithCustomerExcludingAssessedTax','Revenues'})
    with gzip.open(root/'documents'/(SOURCE_SHA256+'.html.gz'),'rb') as f:adjustments=adjustment_source(f.read(32*1024**2+1))
    target=adjustments[r['end']]
    if Decimal(r['originalValue'])!=Decimal(target['previous']) or Decimal(r['value'])!=Decimal(target['current']):raise ValueError('EXTR_REVIEW_SOURCE_ENDPOINTS725')
    checked=reviewed_approval(scope,{**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD','revenueStandardAdjustment':target['adjustment']})
    nodes=r['allPrimaryEvidence']['nodes'];urls=[n['statement']['url'] for n in nodes];dates=[n['filingDate'] for n in nodes]
    if len(nodes)!=3 or len(set(urls))!=3 or dates!=sorted(set(dates)) or dates[-1]!=r['filingDate'] or {x['source_url'] for x in scope}!=set(urls) or urls[-1]!=URL:raise ValueError('EXTR_ALL_SOURCE_REPORTS725')
    vectors=[]
    for node in nodes:
        e=node['statement']
        if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:raise ValueError('EXTR_PRIMARY_DOCUMENT_IDENTITY725')
        rows,ctx,_=document(e,root);vector=primary_vector(rows,ctx)
        if r['end'] not in vector or max(vector)>node['filingDate']:raise ValueError('EXTR_PRIMARY_TARGET_FUTURE725')
        vectors.append(vector)
    overlaps=[]
    for i,(before,after) in enumerate(zip(vectors,vectors[1:])):
        common=sorted(set(before)&set(after))
        if len(common)<2:raise ValueError('EXTR_ALL_COMPARATIVE_COVERAGE725')
        if any(after[day]-before[day]!=(Decimal(adjustments[day]['adjustment']) if i==1 and day in adjustments else Decimal(0)) for day in common):raise ValueError('EXTR_ALL_COMPARATIVE_EQUATIONS725')
        overlaps.append(common)
    for row in scope:
        shadow=dict(row,report_date=row['period_end'])
        if shadow['source_tag']=='SalesRevenueNet':shadow['source_tag']='Revenues'
        if invalid(shadow,annual_only=True):raise ValueError('EXTR_ORIGINAL_ANNUAL_POLICY725')
        i=urls.index(row['source_url'])
        if str(row['filing_date'])!=dates[i] or Decimal(str(row['value']))!=vectors[i][r['end']]:raise ValueError('EXTR_EVERY_RAW_REPORT_VALUE725')
    proof={**checked['proof'],'policy':POLICY,'review':r,'originalRawEvidence':original,'adjustments':adjustments,'allPrimaryComparativeEnds':overlaps,'allSourceReportsBound':True,'originalObservationsPreserved':True,'otherMetricApproval':False,'accountingCause':'EXPLICIT_ASC606_FULL_RETROSPECTIVE_ADOPTION_SHARED_UNIT_SCOPE'}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
