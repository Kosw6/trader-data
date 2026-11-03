"""Explicit support-service revenue errors; never manufacture corrected facts."""
from decimal import Decimal
from pipeline.retail_coverage706 import plain_document
from pipeline.legacy_statement_total699 import document, statement
from pipeline.bound_reconciliation685 import norm, period_years
from pipeline.reconciliation_review679 import numeric
from pipeline.reviewed_financial_mapping672 import reviewed_approval,digest
from pipeline.financial_mapping667 import invalid

BASIS='REVIEWED_EXPLICIT_SERVICE_REVENUE_ERROR_CORRECTION'
POLICY='sec-reviewed-service-error-correction708-v1'


def correction(entry,root,e,end):
    if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:
        raise ValueError('SERVICE_ERROR_DOCUMENT_BINDING')
    text=plain_document(root,e['sha256']);quote=norm(e['causeExcerpt'])
    if not 180<=len(quote)<=2500 or quote not in text or not all(w in quote for w in ['immaterial error','commencement of revenue recognition','forticare support service contracts','understatement of revenue']):
        raise ValueError('SERVICE_ERROR_EXPLICIT_CAUSE_REQUIRED')
    rows,context,_=document(e,root)
    if not norm(context).endswith('the corrections to our consolidated statements of income for the year ended december 31, 2019 and december 31, 2018 were as follows (in millions, except per share amounts):'):
        raise ValueError('SERVICE_ERROR_ANNUAL_UNIT_CONTEXT')
    if len(rows)<14 or norm(' '.join(rows[1]))!='year ended december 31,' or period_years(rows[2])!=['2019','2018']:
        raise ValueError('SERVICE_ERROR_ANNUAL_COLUMNS')
    if [norm(x) for x in rows[3] if x.strip()]!=['as previously reported','corrections','as corrected']*2:
        raise ValueError('SERVICE_ERROR_HEADER_ORDER')
    if end not in {'2018-12-31','2019-12-31'}:raise ValueError('SERVICE_ERROR_PERIOD_NOT_PRINTED')
    labels={5:'service',6:'total revenue',8:'service',9:'total gross profit',10:'operating income',11:'income before income taxes',12:'provision for income taxes',13:'net income'}
    values={}
    for i,label in labels.items():
        if norm(rows[i][0])!=label:raise ValueError('SERVICE_ERROR_STATEMENT_ROW')
        a=[numeric(c) for c in rows[i][1:] if numeric(c) is not None]
        if len(a)!=6 or any(not v.is_finite() for v in a) or any(a[j]+a[j+1]!=a[j+2] for j in [0,3]):
            raise ValueError('SERVICE_ERROR_EXACT_ALL_COLUMN_EQUATION')
        values[i]=a
    for j in [0,3]:
        delta=values[6][j+1]
        if delta<=0 or any(values[i][j+1]!=delta for i in [5,8,9,10,11]) or values[11][j+1]-values[12][j+1]!=values[13][j+1]:
            raise ValueError('SERVICE_ERROR_CROSS_STATEMENT_EQUATIONS')
    j=0 if end.startswith('2019') else 3
    return dict(previouslyReportedUSD=str(values[6][j]*1000000),correctionUSD=str(values[6][j+1]*1000000),correctedUSD=str(values[6][j+2]*1000000),tableHash=e['tableHash'],allComparativeColumnsVerified=True)


def approve(scope,entry,root):
    r=entry['review'];a,b=Decimal(r['originalValue']),Decimal(r['value'])
    if r['basis']!=BASIS or a==b:raise ValueError('SERVICE_ERROR_BASIS')
    checked=reviewed_approval(scope,{**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD','revenueStandardAdjustment':str(b-a)})
    e=r['serviceErrorEvidence'];corr=correction(entry,root,e['correction'],r['end'])
    if (Decimal(corr['previouslyReportedUSD']),Decimal(corr['correctedUSD']))!=(a,b):raise ValueError('SERVICE_ERROR_TARGET_EQUATION')
    for role,sources,value in [('original',checked['proof']['originalSources'],a),('selected',checked['proof']['sources'],b)]:
        s=e[role]
        if {x['source_url'] for x in sources}!={s['url']}:raise ValueError('SERVICE_ERROR_SOURCE_REPORT')
        statement(entry,root,s,r['end'],value,_check_competing=False)
    if e['correction']['url']!=e['selected']['url']:raise ValueError('SERVICE_ERROR_SELECTED_REPORT')
    from pipeline.primary_precision707 import vector
    nodes=e['nodes'];urls=[n['statement']['url'] for n in nodes]
    if len(set(urls))!=len(urls) or set(urls)!={row['source_url'] for row in scope}:
        raise ValueError('SERVICE_ERROR_ALL_RAW_REPORTS_REQUIRED')
    vectors={n['statement']['url']:(vector(entry,root,n)[0],n['filingDate']) for n in nodes}
    for row in scope:
        shadow=dict(row,report_date=row['period_end'])
        if row['source_tag']=='SalesRevenueNet':shadow['source_tag']='Revenues'
        if invalid(shadow,annual_only=True):raise ValueError('SERVICE_ERROR_RAW_ANNUAL')
        v,filed=vectors[row['source_url']]
        if v.get(r['end'])!=Decimal(str(row['value'])) or filed!=str(row['filing_date']):raise ValueError('SERVICE_ERROR_RAW_PRIMARY_VALUE')
        if row['source_url']==e['original']['url'] and Decimal(str(row['value']))!=a:raise ValueError('SERVICE_ERROR_ORIGINAL_RAW_CONFLICT')
        if row['source_url']==e['selected']['url'] and Decimal(str(row['value']))!=b:raise ValueError('SERVICE_ERROR_SELECTED_RAW_CONFLICT')
    proof={**checked['proof'],'policy':POLICY,'review':r,'correctionTable':corr,'accountingCause':'EXPLICIT_SUPPORT_SERVICE_REVENUE_RECOGNITION_ERROR','originalObservationsPreserved':True,'correctedRawFactAlreadyStored':True}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
