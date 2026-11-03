"""Registered annual revenue representation bridge, never a guessed accounting cause."""
import gzip,hashlib,html,re
from decimal import Decimal
from pipeline.reconciliation_review679 import Tables,numeric
from pipeline.bound_reconciliation685 import norm,period_years
from pipeline.reviewed_financial_mapping672 import reviewed_approval,digest

BASIS='REVIEWED_DISCONTINUED_REVENUE_REPRESENTATION'
POLICY='sec-discontinued-revenue-bridge687-v1'

def representation_cause(text):
    text=norm(text)
    # A cash-flow/derivative reclassification into a discontinued-income
    # column is not evidence that business revenue was represented.
    return bool(re.search(r'(?:results(?: of operations)?|operating results|financial (?:statements|information)|business|segment|sales|revenues?).{0,320}(?:classif|present|report|reflect|segregat).{0,160}discontinued operation',text)
                or re.search(r'discontinued operation.{0,260}(?:all (?:prior )?periods|retrospect|recast)',text))


def approve(scope,entry,root):
    r=entry['review'];e=r.get('discontinuedEvidence',{})
    if r.get('basis')!=BASIS:raise ValueError('DISCONTINUED_BASIS_REQUIRED')
    a,b,removed=[Decimal(r[k]) for k in ['originalValue','value','discontinuedRevenue']]
    if not all(v.is_finite() for v in (a,b,removed)) or removed<=0 or a-removed!=b:
        raise ValueError('DISCONTINUED_EXACT_BRIDGE_MISMATCH')
    checked=reviewed_approval(scope,{**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD','revenueStandardAdjustment':str(b-a)})
    selected=checked['proof']['sources'];url=e.get('url');sha=e.get('sha256')
    if not any(x['source_url']==url for x in selected) or not any(d['url']==url and d['sha256']==sha for d in entry['documents']):
        raise ValueError('DISCONTINUED_LATEST_DOCUMENT_REQUIRED')
    with gzip.open(root/'documents'/(sha+'.html.gz'),'rb') as f:body=f.read(32*1024**2+1)
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=sha:raise ValueError('DISCONTINUED_DOCUMENT_CHANGED')
    text=body.decode('utf8',errors='replace');text=re.sub(r'<(script|style)\b[^>]*>.*?</\1>',' ',text,flags=re.S|re.I)
    cause_body=text
    causal=e.get('causeDocument')
    if causal:
        c_url,c_sha=causal.get('url'),causal.get('sha256')
        if not any(d['url']==c_url and d['sha256']==c_sha for d in entry['documents']):raise ValueError('DISCONTINUED_CAUSAL_DOCUMENT_BINDING')
        causal_rows=[x for x in scope if x['source_url']==c_url and x['source_tag']==r['chosenTag'] and x['form_type'] in {'10-K','10-K/A'}]
        if not causal_rows or {Decimal(str(x['value'])) for x in causal_rows}!={b}:raise ValueError('DISCONTINUED_CAUSAL_VALUE_CHAIN')
        if not min(str(x['filing_date']) for x in checked['proof']['originalSources'])<=min(str(x['filing_date']) for x in causal_rows)<=r['filingDate']:raise ValueError('DISCONTINUED_CAUSAL_CHRONOLOGY')
        with gzip.open(root/'documents'/(c_sha+'.html.gz'),'rb') as f:c_body=f.read(32*1024**2+1)
        if len(c_body)>32*1024**2 or hashlib.sha256(c_body).hexdigest()!=c_sha:raise ValueError('DISCONTINUED_CAUSAL_DOCUMENT_CHANGED')
        cause_body=re.sub(r'<(script|style)\b[^>]*>.*?</\1>',' ',c_body.decode('utf8',errors='replace'),flags=re.S|re.I)
    plain=norm(html.unescape(re.sub('<[^>]+>',' ',cause_body)));cause=norm(e.get('causeExcerpt',''))
    if len(cause)<60 or cause not in plain or 'discontinued operations' not in cause or not re.search(r'all (?:prior )?periods|reclassif|recast|segregat',cause) or not representation_cause(cause):
        raise ValueError('DISCONTINUED_EXPLICIT_REPRESENTATION_REQUIRED')
    parser=Tables();parser.feed(text);matches=[(rows,c) for rows,c in zip(parser.tables,parser.contexts) if digest(rows)==e.get('tableHash')]
    if len(matches)!=1:raise ValueError('DISCONTINUED_EXACT_TABLE_REQUIRED')
    rows,context=matches[0];nearby=norm(e.get('tableContextExcerpt',''))
    if len(nearby)<30 or nearby not in norm(context) or 'discontinued operations' not in nearby:
        raise ValueError('DISCONTINUED_COMPONENT_TABLE_CONTEXT_REQUIRED')
    yr,vr=e.get('yearRow'),e.get('valueRow')
    if type(yr) is not int or type(vr) is not int or not 0<=yr<vr<len(rows):raise ValueError('DISCONTINUED_ROW_POSITION')
    years=period_years(rows[yr]);target=str(e.get('fiscalYear'))
    if not years or len(years)>3 or len(set(years))!=len(years) or years.count(target)!=1:
        raise ValueError('DISCONTINUED_PERIOD_COLUMNS')
    if {str(x['fiscal_year']) for x in checked['proof']['originalSources']}!={target}:raise ValueError('DISCONTINUED_FISCAL_YEAR_MISMATCH')
    if any(period_years(head) for head in rows[yr+1:vr]):raise ValueError('DISCONTINUED_CROSS_PERIOD_BINDING')
    row=rows[vr]
    if not row or norm(row[0])!=norm(e.get('rowLabel','')) or not re.fullmatch(r'(?:(?:net|total|operating)\s+)*(?:revenues?|sales)',norm(row[0])):
        raise ValueError('DISCONTINUED_REVENUE_ROW_REQUIRED')
    # SEC tables use a literal em dash for an explicit zero in a year column.
    # Keep its position. Empty cells and arbitrary dashes remain non-values;
    # they cannot repair a missing period or a malformed numeric amount.
    values=[numeric(cell) if numeric(cell) is not None else Decimal(0)
            for cell in row if numeric(cell) is not None or cell.strip()=='\u2014']
    if len(values)!=len(years):raise ValueError('DISCONTINUED_COLUMN_COUNT')
    scale=Decimal(str(e.get('unitScale',0)));units={Decimal(1000):'in thousands',Decimal(1000000):'in millions'}
    table=norm(' '.join(' '.join(row) for row in rows))
    if scale not in units or units[scale] not in norm(context+' '+table):raise ValueError('DISCONTINUED_UNIT_REQUIRED')
    if values[years.index(target)]*scale!=removed:raise ValueError('DISCONTINUED_COMPONENT_AMOUNT_MISMATCH')
    proof={**checked['proof'],'policy':POLICY,'review':r,'accountingCause':BASIS,
           'bridge':{'previousPublishedRevenue':str(a),'latestContinuingRevenue':str(b),'latestDiscontinuedRevenue':str(removed),'samePeriodTotal':str(b+removed)},
           'meaning':'Explicit discontinued-operations representation and exact published total bridge. Not organic growth or a claim about stand-alone spun-off company revenue.'}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
