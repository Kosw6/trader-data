"""Strict non-revenue identity building block, never a standalone approval.

Only a separately registered accounting contract may call this function and
then verify its complete primary statements. No generic metric approval route
is exposed here.
"""
import re
from datetime import date
from decimal import Decimal
from urllib.parse import urlsplit
from pipeline.reviewed_financial_mapping672 import digest


def checked_identity(scope,r,*,company,cik,metric,tag,periods):
    if r['companyId']!=company or r['metric']!=metric or r['chosenTag']!=tag or (r['start'],r['end']) not in periods:
        raise ValueError('REVIEWED_METRIC_EXPLICIT_SCOPE726')
    if not scope or len(scope)>128 or digest(scope)!=r['sourceScopeHash']:
        raise ValueError('REVIEWED_METRIC_STORED_SCOPE726')
    start,end=date.fromisoformat(r['start']),date.fromisoformat(r['end']);asof=date.fromisoformat(r['reviewedOn'])
    if not 335<=(end-start).days<=395 or end>asof:
        raise ValueError('REVIEWED_METRIC_ANNUAL_PERIOD726')
    unit='USD/shares' if metric=='DILUTED_EPS' else 'USD'
    for row in scope:
        if (row['company_id'],row['metric_id'],row['metric_code'],str(row['cik']).lstrip('0'),row['taxonomy'],row['source_tag'],row['unit'],str(row['period_start']),str(row['period_end']))!=(company,r['metricId'],metric,str(int(cik)),'us-gaap',tag,unit,r['start'],r['end']) or row['dimensions'] not in (None,{}):
            raise ValueError('REVIEWED_METRIC_SOURCE_IDENTITY726')
        if row['form_type'] not in {'10-K','10-K/A'} or str(row['filing_date'])>r['reviewedOn'] or str(row['filing_date'])<r['end']:
            raise ValueError('REVIEWED_METRIC_ANNUAL_FILING726')
        url=urlsplit(row['source_url']);prefix='/Archives/edgar/data/'+str(int(cik))+'/'+row['accession_number'].replace('-','')+'/'
        if url.scheme!='https' or url.netloc!='www.sec.gov' or not url.path.startswith(prefix) or '..' in url.path or url.query or url.fragment or not re.fullmatch('[a-f0-9]{64}',row['source_fact_hash']):
            raise ValueError('REVIEWED_METRIC_SOURCE_PROVENANCE726')
        if not Decimal(str(row['value'])).is_finite():raise ValueError('REVIEWED_METRIC_NONFINITE726')
    selected=[row for row in scope if str(row['filing_date'])==max(str(x['filing_date']) for x in scope)]
    original=[row for row in scope if str(row['report_date'])==r['end']]
    if not original:raise ValueError('REVIEWED_METRIC_ORIGINAL_ANNUAL_REQUIRED726')
    original=[row for row in original if str(row['filing_date'])==max(str(x['filing_date']) for x in original)]
    if {Decimal(str(x['value'])) for x in selected}!={Decimal(r['value'])} or {Decimal(str(x['value'])) for x in original}!={Decimal(r['originalValue'])}:
        raise ValueError('REVIEWED_METRIC_ENDPOINT_VALUES726')
    if {str(x['filing_date']) for x in selected}!={r['filingDate']} or {x['accession_number'] for x in selected}!=set(r['accessions']) or {x['source_fact_hash'] for x in selected}!=set(r['sourceFactHashes']):
        raise ValueError('REVIEWED_METRIC_LATEST_SOURCE_SELECTION726')
    if set(r['officialDocuments'])!={x['source_url'] for x in scope} or not r.get('reviewExplanation'):
        raise ValueError('REVIEWED_METRIC_ALL_SOURCE_DOCUMENTS726')
    return dict(company_id=company,metric_id=r['metricId'],source_tag=tag,end=r['end'],
                sources=selected,originalSources=original)
