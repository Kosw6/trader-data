"""Operator-registered same-report presentation precision, never error correction."""
import gzip,hashlib,re
from decimal import Decimal,ROUND_HALF_UP
from pipeline.reviewed_financial_mapping672 import reviewed_approval,digest
from pipeline.reconciliation_review679 import Tables,numeric
BASIS='EXPLICIT_PRESENTATION_PRECISION'
POLICY='sec-reviewed-presentation-precision682-v1'

def verify_binding(binding,root,value,review):
    sha=binding['sha256']
    if not re.fullmatch('[a-f0-9]{64}',sha):raise ValueError('PRECISION_DOCUMENT_HASH_INVALID')
    with gzip.open(root/'documents'/(sha+'.html.gz'),'rb') as f:body=f.read(32*1024**2+1)
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=sha:raise ValueError('PRECISION_DOCUMENT_CHANGED')
    parser=Tables();parser.feed(body.decode('utf8',errors='replace'))
    matches=[(t,c) for t,c in zip(parser.tables,parser.contexts) if digest(t)==binding['tableHash']]
    if len(matches)!=1:raise ValueError('PRECISION_TABLE_NOT_UNIQUE')
    rows,context=matches[0];header=rows[binding['yearRow']];row=rows[binding['valueRow']]
    years=[y for cell in header for y in re.findall(r'\b(?:19|20)\d{2}\b',cell)]
    fy=str(binding['fiscalYear'])
    if years.count(fy)!=1:raise ValueError('PRECISION_YEAR_NOT_UNIQUE')
    if not re.match(r'^(?:(?:total|net|operating)\s+)*(?:revenues?|sales)\b',row[0],re.I):raise ValueError('PRECISION_REVENUE_ROW_REQUIRED')
    vals=[v for cell in row if (v:=numeric(cell)) is not None]
    if len(vals)!=len(years):raise ValueError('PRECISION_YEAR_VALUE_ALIGNMENT')
    scale=Decimal(str(binding['unitScale']));places=binding['decimalPlaces']
    if scale not in {Decimal(1000),Decimal(1000000)} or places not in {0,1,2}:raise ValueError('PRECISION_UNIT_INVALID')
    unit='thousands' if scale==1000 else 'millions'
    if unit not in (' '.join(' '.join(r) for r in rows[:binding['valueRow']+1])+' '+context).casefold():raise ValueError('PRECISION_UNIT_NOT_CONFIRMED')
    shown=vals[years.index(fy)]
    if shown*scale!=value:raise ValueError('PRECISION_SHOWN_VALUE_MISMATCH')
    numeric_cells=[cell for cell in row if numeric(cell) is not None]
    display=numeric_cells[years.index(fy)].strip().replace('$','').replace(',','').strip('()').strip()
    actual_places=len(display.split('.')[1]) if '.' in display else 0
    if actual_places!=places:raise ValueError('PRECISION_DISPLAY_DECIMALS_MISMATCH')
    quantum=scale/(Decimal(10)**places)
    if quantum!=Decimal(str(review['precisionQuantum'])):raise ValueError('PRECISION_QUANTUM_MISMATCH')

def precision_approval(scope,entry,root):
    r=entry['review'];e=r['precisionEvidence'];a=Decimal(r['originalValue']);b=Decimal(r['value']);q=Decimal(str(r['precisionQuantum']))
    if r['basis']!=BASIS or q not in {Decimal(10)**i for i in range(1,7)}:raise ValueError('PRECISION_BASIS_OR_QUANTUM_INVALID')
    if (a/q).quantize(Decimal(1),rounding=ROUND_HALF_UP)*q!=b:raise ValueError('PRECISION_ROUNDING_MISMATCH')
    original=[x for x in scope if x['source_tag']=='SalesRevenueNet' and str(x['report_date'])==r['end'] and x['form_type'] in {'10-K','10-K/A'}]
    latest=[x for x in scope if x['accession_number'] in r['accessions'] and x['source_tag']==r['chosenTag']]
    docs={(d['url'],d['sha256']) for d in entry['documents']}
    for role,sources in [('originalRounded',original),('latestRounded',latest)]:
        v=e[role]
        if ((v['url'],v['sha256']) not in docs or not any(x['source_url']==v['url'] for x in sources)
            or not any(str(x['fiscal_year'])==str(v['fiscalYear']) for x in original)):raise ValueError('PRECISION_SAME_REPORT_BRIDGE_REQUIRED')
        verify_binding(v,root,b,r)
    shadow={**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD','revenueStandardAdjustment':str(b-a)}
    checked=reviewed_approval(scope,shadow)
    proof={**checked['proof'],'policy':POLICY,'review':r,'accountingCause':BASIS,'meaning':'Published precision agrees already in original report; not proof of unchanged economics below reporting precision.'}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
