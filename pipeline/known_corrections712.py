"""Exact reviewed later-correction scopes, not corrected financial values."""
SOURCE_SHA256='bf590297ab073dabb7358f6fa3f1c6bb27e407e635579d40d247fcd2206802b6'
TABLE_SHA256='8adad7f6e7cb2e4aa7f6d627b02ceb5f0945203224828e67fa44a57174679abd'
SCOPES=frozenset((1919,metric,f'{year}-01-01',f'{year}-12-31') for metric in ('REVENUE','NET_INCOME','OPERATING_INCOME','DILUTED_EPS') for year in (2016,2017))
REASON='KNOWN_LATER_DOCUMENT_CORRECTION712'

def blocked_row(row):
    return (row.get('company_id'),row.get('metric_code'),str(row.get('period_start')),str(row.get('period_end'))) in SCOPES

def verify_source(body):
    import hashlib
    from decimal import Decimal
    from pipeline.reconciliation_review679 import Tables,numeric
    from pipeline.bound_reconciliation685 import norm,period_years
    from pipeline.reviewed_financial_mapping672 import digest
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=SOURCE_SHA256:raise ValueError('LATE_CORRECTION_SOURCE_CHANGED')
    p=Tables();p.feed(body.decode('utf8','replace'));found=[]
    for rows,context in zip(p.tables,p.contexts):
        if digest(rows)!=TABLE_SHA256:continue
        if len(rows)!=14 or period_years(rows[2])!=['2017','2016'] or norm(' '.join(rows[1]))!='year ended december 31,':raise ValueError('LATE_CORRECTION_PERIOD_HEADER')
        if [norm(c) for c in rows[3] if c.strip()]!=['as previously reported','corrections','as corrected']*2:raise ValueError('LATE_CORRECTION_COLUMNS')
        if norm(' '.join(rows[5]))!='(in millions, except per share amounts)' or norm(rows[6][0])!='consolidated statements of income data:':raise ValueError('LATE_CORRECTION_UNIT_AND_STATEMENT')
        if not all(x in norm(context) for x in ['immaterial error','revenue recognition','forticare support service contracts','understatement of revenue']):raise ValueError('LATE_CORRECTION_CAUSE')
        if norm(rows[11][0])!='net income per share:' or norm(rows[12][0])!='basic':raise ValueError('LATE_CORRECTION_EPS_HIERARCHY')
        for metric,i,label in [('REVENUE',7,'total revenue'),('OPERATING_INCOME',9,'operating income'),('NET_INCOME',10,'net income'),('DILUTED_EPS',13,'diluted')]:
            if norm(rows[i][0])!=label:raise ValueError('LATE_CORRECTION_METRIC_ROW')
            values=[numeric(c) for c in rows[i][1:] if numeric(c) is not None]
            if len(values)!=6 or any(not v.is_finite() for v in values):raise ValueError('LATE_CORRECTION_VALUE_COLUMNS')
            scale=Decimal(1) if metric=='DILUTED_EPS' else Decimal(1000000)
            for j,year in [(0,2017),(3,2016)]:
                a,d,b=values[j:j+3]
                if a+d!=b or d==0:raise ValueError('LATE_CORRECTION_EXACT_EQUATION')
                found.append(dict(companyId=1919,metric=metric,start=f'{year}-01-01',end=f'{year}-12-31',previous=str(a*scale),correction=str(d*scale),corrected=str(b*scale),unit='USD/shares' if metric=='DILUTED_EPS' else 'USD',documentSha256=SOURCE_SHA256,tableHash=TABLE_SHA256,approvalGenerated=False))
    if {(r['companyId'],r['metric'],r['start'],r['end']) for r in found}!=SCOPES:raise ValueError('LATE_CORRECTION_REGISTRY_MISMATCH')
    return found
