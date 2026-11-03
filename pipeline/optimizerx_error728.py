"""Two named 2012 loss metrics, supported by OptimizeRx Note 18 correction.

The correction reverses cash-basis revenue-share expense recognition to accrual.
It does not authorize any revenue presentation bridge or another company's facts.
"""
from decimal import Decimal
from pipeline.legacy_statement_total699 import document,clean
from pipeline.reviewed_metric_identity726 import checked_identity
from pipeline.original_sec_scope725 import verify
from pipeline.reviewed_financial_mapping672 import digest

BASIS='REVIEWED_OPTIMIZERX_REVENUE_SHARE_EXPENSE_ERROR728'
POLICY='sec-optimizerx-revenue-share-expense-error728-v1'
RAW_SHA256='0e0ccb6a17439b44ea3adee495cb8c7df67c5fcf77bf8c2402ced6fef22c4f7c'
METRICS={'NET_INCOME':(23,'NetIncomeLoss'),'OPERATING_INCOME':(24,'OperatingIncomeLoss')}
PERIODS={('2012-01-01','2012-12-31')}
CLAUSES=('during the quarter ended december 31, 2013, the company corrected an accounting error related to revenue share expenses.',
         'the company will accrue the expense for amounts due, versus when they are paid, for proper matching.',
         'the revenue share expense was not properly accrued at the end of 2012 or at the end of each 2013 quarter')

def metadata(company,r):
    if company!=2470 or r['companyId']!=2470 or (r['start'],r['end']) not in PERIODS or METRICS.get(r['metric'])!=(r['metricId'],r['chosenTag']):
        raise ValueError('OPTIMIZERX_NAMED_LOSS_SCOPE728')

def amounts(cells):
    result=[];i=0
    while i<len(cells):
        text=cells[i].strip();i+=1
        if text in {'','$'}:continue
        if text.startswith('(') and not text.endswith(')'):
            while i<len(cells) and not cells[i].strip():i+=1
            if i==len(cells) or cells[i].strip()!=')':raise ValueError('OPTIMIZERX_NEGATIVE_CELL_CLOSURE728')
            text+=')';i+=1
        import re
        if text=='-0-':value=Decimal(0)
        elif re.fullmatch(r'\(?\d{1,3}(?:,\d{3})*\)?',text) and text.startswith('(')==text.endswith(')'):
            value=Decimal(text.strip('()').replace(',',''))*(-1 if text.startswith('(') else 1)
        else:raise ValueError('OPTIMIZERX_LITERAL_USD_CELL728')
        result.append(value)
    return result

def primary(rows,context,restated):
    ctx=clean(context);suffix='optimizerx corporation consolidated statements of operations for the years ended december 31, '+('2013 and 2012' if restated else '2012 and 2011')
    if not ctx.endswith(suffix) or 'in thousands' in ctx[-300:] or 'in millions' in ctx[-300:]:raise ValueError('OPTIMIZERX_EXACT_PRIMARY_TITLE728')
    expected=[['','','For the','','year ended'],['','','year ended','','December 31,'],['','','December 31,','','2012'],['','','2013','','(restated)']] if restated else [['','','For the','','For the'],['','','year ended','','year ended'],['','','December 31,','','December 31,'],['','','2012','','2011']]
    prefix=rows[:5] if restated else rows[:4]
    # Restated source has a separate leading line for the right-hand year.
    if restated:expected=[['','','','','For the'],*expected]
    if [[clean(c) for c in r] for r in prefix]!=[[clean(c) for c in r] for r in expected]:raise ValueError('OPTIMIZERX_EXACT_TWO_YEAR_HEADER728')
    labels={'op':'income (loss) from operations' if restated else 'loss from operations','ni':'net income (loss)' if restated else 'net loss','pretax':'income (loss) before provision for income taxes' if restated else 'loss before provision for income taxes','other':'total other income (expense)','expense':'operating expenses (see note 17)','revenue':'net revenue' if restated else 'total revenue'}
    values={}
    for key,label in labels.items():
        hits=[r for r in rows if r and clean(r[0])==label]
        if len(hits)!=1:raise ValueError('OPTIMIZERX_PRIMARY_LINE_IDENTITY728')
        line=hits[0]
        if key=='ni' and '$' not in line:raise ValueError('OPTIMIZERX_PRIMARY_DOLLAR_MARK728')
        values[key]=amounts(line[1:])
        if len(values[key])!=2:raise ValueError('OPTIMIZERX_PRIMARY_COLUMN_COUNT728')
    if restated:
        for key,label in [('shareExpense','revenue share expense'),('gross','gross margin')]:
            hits=[r for r in rows if r and clean(r[0])==label]
            if len(hits)!=1:raise ValueError('OPTIMIZERX_PRIMARY_GROSS_HIERARCHY728')
            values[key]=amounts(hits[0][1:])
            if len(values[key])!=2:raise ValueError('OPTIMIZERX_PRIMARY_COLUMN_COUNT728')
    for i in range(2):
        gross=values['gross'][i] if restated else values['revenue'][i]
        if restated and values['revenue'][i]-values['shareExpense'][i]!=gross:raise ValueError('OPTIMIZERX_PRIMARY_GROSS_EQUATION728')
        if gross-values['expense'][i]!=values['op'][i] or values['op'][i]+values['other'][i]!=values['pretax'][i] or values['pretax'][i]!=values['ni'][i]:raise ValueError('OPTIMIZERX_PRIMARY_FULL_LOSS_EQUATIONS728')
    years=[2013,2012] if restated else [2012,2011]
    return {str(y)+'-12-31':{k:v[i] for k,v in values.items()} for i,y in enumerate(years)}

def correction(rows,context):
    ctx=clean(context)
    if any(ctx.count(c)!=1 for c in CLAUSES) or 'note 18' not in ctx or not ctx.endswith('the restated consolidated balance sheet, consolidated statement of operations and consolidated statement of cash flows for the year ended december 31, 2012 is as follows:'):
        raise ValueError('OPTIMIZERX_BOUND_ERROR_DECLARATION728')
    if [clean(c) for c in rows[0]]!=['year ended december 31, 2012','','','',''] or [clean(c) for c in rows[1]]!=['financial statement','','line item','','','corrected','','','','previously stated','']:
        raise ValueError('OPTIMIZERX_CORRECTED_PREVIOUS_COLUMN_DIRECTION728')
    expected=[('balance sheet','revenue share payable'),('income statement','revenue share expense'),('income statement','loss from operations'),('income statement','loss before provision for income taxes'),('income statement','net loss'),('statement of cash flows','net loss'),('statement of cash flows','increase in revenue share payable')]
    if len(rows)!=9:raise ValueError('OPTIMIZERX_COMPLETE_ERROR_TABLE728')
    lines=[]
    for row,(statement,label) in zip(rows[2:],expected):
        if len(row)<4 or clean(row[0])!=statement or clean(row[2])!=label or row.count('$')!=2:raise ValueError('OPTIMIZERX_ERROR_LINE_UNIT_IDENTITY728')
        values=amounts(row[3:])
        if len(values)!=2:raise ValueError('OPTIMIZERX_ERROR_VALUE_COLUMNS728')
        lines.append(values)
    delta=lines[0][0]-lines[0][1]
    if delta<=0 or lines[1][0]-lines[1][1]!=delta or lines[6]!=lines[0] or any(new-old!=-delta for new,old in lines[2:6]):raise ValueError('OPTIMIZERX_ACCRUAL_CORRECTION_EQUATIONS728')
    return dict(delta=delta,op=lines[2],pretax=lines[3],ni=lines[4],shareExpense=lines[1])

def approve(scope,entry,root):
    r=entry['review'];metadata(r['companyId'],r)
    if r['basis']!=BASIS or str(int(entry['cik']))!='1448431':raise ValueError('OPTIMIZERX_COMPANY_BASIS728')
    checked=checked_identity(scope,r,company=2470,cik='0001448431',metric=r['metric'],tag=r['chosenTag'],periods=PERIODS)
    raw=verify(scope,r['originalRawEvidence'],root,company=2470,cik='0001448431',raw_sha256=RAW_SHA256,metric=r['metric'],tags={r['chosenTag']})
    evidence=r['expenseErrorEvidence'];nodes=evidence['nodes'];urls=[n['statement']['url'] for n in nodes];dates=[n['filingDate'] for n in nodes]
    if len(nodes)!=2 or len(set(urls))!=2 or dates!=sorted(set(dates)) or set(urls)!={x['source_url'] for x in scope} or dates[-1]!=r['filingDate']:raise ValueError('OPTIMIZERX_ALL_ORIGINAL_REPORTS728')
    vectors=[]
    for i,n in enumerate(nodes):
        e=n['statement']
        if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:raise ValueError('OPTIMIZERX_DOCUMENT_BINDING728')
        rows,ctx,_=document(e,root);v=primary(rows,ctx,i==1)
        if r['end'] not in v or max(v)>n['filingDate']:raise ValueError('OPTIMIZERX_PRIMARY_PERIOD728')
        vectors.append(v)
    c=evidence['correction']
    if (c['url'],c['sha256'])!=(nodes[1]['statement']['url'],nodes[1]['statement']['sha256']):raise ValueError('OPTIMIZERX_LATEST_CORRECTION_SOURCE728')
    rows,ctx,_=document(c,root);bridge=correction(rows,ctx)
    a,b=[v[r['end']] for v in vectors]
    if bridge['op']!=[b['op'],a['op']] or bridge['ni']!=[b['ni'],a['ni']] or bridge['pretax']!=[b['pretax'],a['pretax']] or bridge['shareExpense'][0]!=b['shareExpense']:
        raise ValueError('OPTIMIZERX_PRIMARY_CORRECTION_ENDPOINTS728')
    key='ni' if r['metric']=='NET_INCOME' else 'op'
    for row in scope:
        i=urls.index(row['source_url'])
        if str(row['filing_date'])!=dates[i] or str(row['report_date'])!=max(vectors[i]) or Decimal(str(row['value']))!=vectors[i][r['end']][key]:raise ValueError('OPTIMIZERX_EVERY_ORIGINAL_VALUE_PERIOD728')
    proof=dict(policy=POLICY,review=r,sources=checked['sources'],originalSources=checked['originalSources'],originalRawEvidence=raw,
               deltaUSD=str(bridge['delta']),originalObservationsPreserved=True,allSourceReportsBound=True,
               accountingCause='EXPLICIT_REVENUE_SHARE_EXPENSE_ACCRUAL_ERROR',
               meaning='Only named 2012 net and operating loss. Note 18 payable/expense/loss/cash-flow correction and both complete primary statements are required. Revenue presentation changes are not approved.')
    return {k:checked[k] for k in ['company_id','metric_id','source_tag','end']}|dict(proof=proof,proof_hash=digest(proof))
