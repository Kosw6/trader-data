"""Pinned Yelp ASC606 source equations, deliberately not an approval policy.

Current DB scope and all source statements must be revalidated before any
future mapping. No integration into validate_entry or serving is provided.
"""
import hashlib,re
from decimal import Decimal
from pipeline.reconciliation_review679 import Tables,numeric
from pipeline.legacy_statement_total699 import clean,DATE
from pipeline.reviewed_financial_mapping672 import digest

SOURCE_SHA256='dc0f72cc38af97df642a064ea2efc848747eec49aaa8c5f3462d2fe9c2b0bced'
TABLE_SHA256='d678e0706795b2f30a9f01e943476ae6d4a36338bacd8eea32d171f27f074449'
URL='https://www.sec.gov/Archives/edgar/data/1345016/000162828019002312/yelp_10-kx12312018.htm'


def source_evidence(body):
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=SOURCE_SHA256:raise ValueError('YELP_REVIEW_SOURCE_CHANGED')
    p=Tables();p.feed(body.decode('utf8','replace'));matches=[]
    for rows,context in zip(p.tables,p.contexts):
        if digest(rows)!=TABLE_SHA256:continue
        if len(rows)<14:raise ValueError('YELP_REVIEW_ROWS')
        roles=[clean(c) for c in rows[2] if clean(c)]
        if roles!=['as previously reported','impact of asc 606 adoption','as currently reported']:raise ValueError('YELP_REVIEW_COLUMN_ROLES')
        ctx=clean(context)
        if not all(v in ctx for v in ['adopted asc 606 effective january 1, 2018','using the full retrospective method','recast each prior reporting period presented','following adjustments to its previously reported results (in thousands)']):raise ValueError('YELP_REVIEW_METHOD_UNIT_CONTEXT')
        if set(re.findall(r'\bin (thousands|millions)\b',ctx))!={'thousands'}:raise ValueError('YELP_REVIEW_CONFLICTING_UNIT')
        for year,heading,index in [(2016,3,4),(2017,12,13)]:
            title=clean(' '.join(rows[heading]));dates=list(DATE.finditer(' '.join(rows[heading])))
            if 'consolidated statement of operations' not in title or 'year ended' not in title or len(dates)!=1 or tuple(dates[0].groups())!=('December','31',str(year)):raise ValueError('YELP_REVIEW_ACTUAL_ANNUAL_END')
            if clean(rows[index][0])!='net revenue':raise ValueError('YELP_REVIEW_REVENUE_ROW')
            values=[v for c in rows[index][1:] if (v:=numeric(c)) is not None]
            if len(values)!=3 or values[0]+values[1]!=values[2] or values[1]==0:raise ValueError('YELP_REVIEW_EXACT_EQUATION')
            matches.append(dict(companyId=2149,cik='0001345016',metric='REVENUE',end=f'{year}-12-31',previous=str(values[0]*1000),adjustment=str(values[1]*1000),current=str(values[2]*1000),unit='USD',unitScale='1000',method='ASC606_FULL_RETROSPECTIVE_EXPLICIT',sourceUrl=URL,documentSha256=SOURCE_SHA256,tableHash=TABLE_SHA256,periodHeadingRow=heading,valueRow=index,actualPeriodStartBound=False,requires=['CURRENT_DB_SCOPE_REVALIDATION','ALL_ORIGINAL_AND_LATEST_STATEMENT_BINDING','SERVING_AND_GO_REGRESSION'],approvalGenerated=False,publicationAllowed=False))
    if len(matches)!=2:raise ValueError('YELP_REVIEW_EXACT_TABLE_REQUIRED')
    return matches


def bind_frozen_scope(scope,evidence):
    """Bind frozen raw identifiers only; explicitly retain the live-read blocker."""
    if scope['companyId']!=2149 or scope['metricId']!=22 or scope['metric']!='REVENUE' or scope['end']!=evidence['end'] or str(int(scope['cik']))!='1345016':raise ValueError('YELP_REVIEW_FROZEN_IDENTITY')
    year=evidence['end'][:4];allowed={'SalesRevenueNet':Decimal(evidence['previous']),'RevenueFromContractWithCustomerExcludingAssessedTax':Decimal(evidence['current'])}
    if not scope['rows'] or digest(scope['rows'])!=scope['sourceScopeHash']:raise ValueError('YELP_REVIEW_FROZEN_HASH')
    for r in scope['rows']:
        if r['metric_id']!=22 or str(int(r['cik']))!='1345016' or not r['source_url'].startswith('https://www.sec.gov/Archives/edgar/data/1345016/'):raise ValueError('YELP_REVIEW_FROZEN_SOURCE_IDENTITY')
        if (r['company_id'],r['metric_code'],r['period_start'],r['period_end'],r['taxonomy'],r['unit'],r['form_type'])!=(2149,'REVENUE',year+'-01-01',year+'-12-31','us-gaap','USD','10-K') or r['dimensions'] not in (None,{}):raise ValueError('YELP_REVIEW_FROZEN_PERIOD_OR_POLICY')
        if r['source_tag'] not in allowed or Decimal(r['value'])!=allowed[r['source_tag']]:raise ValueError('YELP_REVIEW_FROZEN_RAW_VALUE')
    return dict(**evidence,sourceScopeHash=scope['sourceScopeHash'],frozenRawRows=len(scope['rows']),frozenExactPeriodStarts=sorted({r['period_start'] for r in scope['rows']}),sourceFactHashes=sorted({r['source_fact_hash'] for r in scope['rows']}),currentDatabaseRevalidated=False)
