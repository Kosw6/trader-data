"""One named exact primary total versus explicitly approximate geographic note.

No global legacy-tag or dollar-unit fallback. The approved value is the literal
total in two primary annual statements; the rounded note and every original
structured observation remain untouched.
"""
import re
from decimal import Decimal,ROUND_HALF_UP
from pipeline.legacy_statement_total699 import clean,document
from pipeline.reviewed_financial_mapping672 import reviewed_approval,digest
from pipeline.original_sec_scope725 import verify

BASIS='REVIEWED_BIOMERICA_EXACT_PRIMARY_APPROXIMATE_GEOGRAPHIC730'
POLICY='sec-biomerica-exact-primary-geographic730-v1'
RAW_SHA256='f2952cf20cd7da1196712785dbe8a4ad5bcd03f9735ec4e36c4eb76903057c53'

def metadata(r):
    if (r['companyId'],r['metricId'],r['metric'],r['start'],r['end'],r['chosenTag'])!=(266,22,'REVENUE','2011-06-01','2012-05-31','SalesRevenueNet'):
        raise ValueError('BIOMERICA_NAMED_EXACT_TOTAL730')

def amounts(cells):
    result=[]
    for c in cells:
        c=c.strip()
        if c in {'','$'}:continue
        if not re.fullmatch(r'\(?\d{1,3}(?:,\d{3})*\)?',c) or c.startswith('(')!=c.endswith(')'):raise ValueError('BIOMERICA_LITERAL_DOLLAR_CELL730')
        result.append(Decimal(c.strip('()').replace(',',''))*(-1 if c.startswith('(') else 1))
    return result

def primary(rows,context):
    if not clean(context).endswith('biomerica, inc. and subsidiaries consolidated statements of operations and comprehensive income (loss)'):
        raise ValueError('BIOMERICA_PRIMARY_CONSOLIDATED_TITLE730')
    if not rows or clean(rows[0][0])!='years ended may 31':raise ValueError('BIOMERICA_EXPLICIT_MAY31_ANNUAL730')
    years=[int(c) for c in rows[0][1:] if c.strip()]
    if len(years)!=2 or years not in [[2012,2011],[2013,2012]]:raise ValueError('BIOMERICA_TWO_NAMED_ANNUAL_COLUMNS730')
    def line(label):
        hits=[]
        for row in rows[1:]:
            nonempty=[(i,c) for i,c in enumerate(row) if c.strip()]
            if nonempty and clean(nonempty[0][1])==label:hits.append((row,nonempty[0][0]))
        if len(hits)!=1:raise ValueError('BIOMERICA_PRIMARY_LINE_HIERARCHY730')
        row,i=hits[0];v=amounts(row[i+1:])
        if len(v)!=2:raise ValueError('BIOMERICA_PRIMARY_VALUE_COLUMNS730')
        if label=='net sales' and row.count('$')!=2:raise ValueError('BIOMERICA_PRIMARY_DOLLAR_MARK730')
        return v
    sales=line('net sales');cost=line('cost of sales');gross=line('gross profit');sga=line('selling, general and administrative');rd=line('research and development');expenses=line('total operating expenses')
    if any(c>=0 or s+c!=g or x+y!=e for s,c,g,x,y,e in zip(sales,cost,gross,sga,rd,expenses)):
        raise ValueError('BIOMERICA_FULL_PRIMARY_EQUATIONS730')
    return {str(y)+'-05-31':s for y,s in zip(years,sales)}

def geographic(rows,context):
    if not clean(context).endswith('8. business segments geographic information regarding net sales is approximately as follows:'):
        raise ValueError('BIOMERICA_EXPLICIT_APPROXIMATE_NOTE730')
    if len(rows)!=9 or [clean(c) for c in rows[0]]!=['','','2012','2011'] or [clean(c) for c in rows[1]]!=['net sales:','','','','']:
        raise ValueError('BIOMERICA_GEOGRAPHIC_YEARS730')
    labels=['europe','united states','asia','south america','middle east','other foreign','total net sales'];values=[]
    for row,label in zip(rows[2:],labels):
        if len(row)<2 or row[0].strip() or clean(row[1])!=label:raise ValueError('BIOMERICA_COMPLETE_GEOGRAPHY730')
        v=amounts(row[2:])
        if len(v)!=2 or any(x<0 or x%1000 for x in v):raise ValueError('BIOMERICA_EXPLICIT_THOUSAND_ROUNDED_NOTE730')
        if label in {'europe','total net sales'} and row.count('$')!=2:raise ValueError('BIOMERICA_NOTE_DOLLAR_MARK730')
        values.append(v)
    if any(sum(v[i] for v in values[:-1])!=values[-1][i] for i in range(2)):raise ValueError('BIOMERICA_ALL_GEOGRAPHIC_SUMS730')
    return dict(zip(['2012-05-31','2011-05-31'],values[-1]))

def approve(scope,entry,root):
    r=entry['review'];metadata(r)
    if r['basis']!=BASIS or str(int(entry['cik']))!='73290':raise ValueError('BIOMERICA_BASIS_CIK730')
    checked=reviewed_approval(scope,{**r,'basis':'UNCHANGED_TOTAL_CONFIRMED'},_reviewed_legacy_total=True)
    raw=verify(scope,r['originalRawEvidence'],root,company=266,cik='0000073290',raw_sha256=RAW_SHA256,metric='REVENUE',tags={'SalesRevenueNet','Revenues'})
    legacy=[x for x in scope if x['source_tag']=='SalesRevenueNet'];modern=[x for x in scope if x['source_tag']=='Revenues']
    if len(legacy)!=2 or len(modern)!=1 or any(Decimal(str(x['value']))!=Decimal(r['value']) for x in legacy):raise ValueError('BIOMERICA_EXACT_LEGACY_OBSERVATIONS730')
    nodes=r['exactPrimaryEvidence']['nodes'];urls=[n['statement']['url'] for n in nodes];dates=[n['filingDate'] for n in nodes]
    if len(nodes)!=2 or len(set(urls))!=2 or dates!=sorted(set(dates)) or set(urls)!={x['source_url'] for x in scope} or dates[-1]!=r['filingDate']:raise ValueError('BIOMERICA_ALL_SOURCE_REPORTS730')
    if {x['source_url'] for x in checked['proof']['originalSources']}!={urls[0]} or {x['source_url'] for x in checked['proof']['sources']}!={urls[-1]}:raise ValueError('BIOMERICA_ENDPOINT_REPORTS730')
    vectors=[]
    for n in nodes:
        e=n['statement']
        if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:raise ValueError('BIOMERICA_DOCUMENT_BINDING730')
        rows,ctx,_=document(e,root);v=primary(rows,ctx)
        if max(v)>n['filingDate'] or v[r['end']]!=Decimal(r['value']):raise ValueError('BIOMERICA_EXACT_PRIMARY_ENDPOINT730')
        vectors.append(v)
    for row in legacy:
        i=urls.index(row['source_url'])
        if str(row['filing_date'])!=dates[i] or str(row['report_date'])!=max(vectors[i]):raise ValueError('BIOMERICA_EVERY_LEGACY_REPORT_PERIOD730')
    note=r['exactPrimaryEvidence']['geographic']
    if (note['url'],note['sha256'])!=(nodes[0]['statement']['url'],nodes[0]['statement']['sha256']) or modern[0]['source_url']!=note['url'] or str(modern[0]['report_date'])!=r['end'] or str(modern[0]['filing_date'])!=dates[0]:raise ValueError('BIOMERICA_APPROXIMATE_SOURCE_IDENTITY730')
    rows,ctx,_=document(note,root);approx=geographic(rows,ctx)
    if set(approx)!=set(vectors[0]) or any((v/1000).quantize(Decimal(1),rounding=ROUND_HALF_UP)*1000!=approx[day] for day,v in vectors[0].items()):raise ValueError('BIOMERICA_BOTH_APPROXIMATE_COMPARATIVES730')
    if Decimal(str(modern[0]['value']))!=approx[r['end']] or approx[r['end']]==Decimal(r['value']):raise ValueError('BIOMERICA_MODERN_APPROXIMATE_VALUE730')
    proof={**checked['proof'],'policy':POLICY,'review':r,'originalRawEvidence':raw,'accountingCause':'EXPLICIT_APPROXIMATE_GEOGRAPHIC_NOTE_VS_EXACT_PRIMARY_TOTAL',
           'allSourceReportsBound':True,'originalObservationsPreserved':True,'conflictingModernSources':modern,'approximateComparativesUSD':{k:str(v) for k,v in approx.items()},
           'restatementClaimed':False,'servingTagContract':'EXPLICIT_COMPANY_PERIOD_LEGACY_STATEMENT_TOTAL699_V1','mappingScope':'COMPANY_METRIC_PERIOD_END_ONLY',
           'meaning':'Only named 2012 revenue. Two primary exact net-sales totals are preserved; an explicitly approximate geographic note is not substituted for the primary total. No global tag priority or other metric approval.'}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
